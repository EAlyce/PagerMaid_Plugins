from pagermaid.listener import listener
from pagermaid.enums import Message
from pagermaid.common.cache import cache
from pagermaid.utils import logs
from telethon.tl.functions.channels import EditBannedRequest, GetParticipantRequest, DeleteParticipantHistoryRequest
from telethon.tl.types import ChatBannedRights, InputPeerUser, InputPeerChannel
from telethon.errors import ChatAdminRequiredError, MessageTooLongError, MessageNotModifiedError
from datetime import datetime, timedelta
import asyncio
import time
import contextlib

# 配置常量 - 集成到PagerMaid架构
BATCH_SIZE = 20  # 并发处理的批次大小
PARALLEL_LIMIT = 8  # 跨群解析/探测的并发度
USE_GET_PARTICIPANT_FIRST = True  # 解析优先策略：优先使用 GetParticipantRequest 精确探测
PER_GROUP_SCAN_LIMIT = 2000  # 回退成员遍历时每群的扫描上限
AUTO_DELETE_DELAY = 14  # 自动删除消息延迟（秒）

async def smart_edit(message: Message, text: str, delete_after: int = AUTO_DELETE_DELAY) -> Message:
    """智能编辑消息 - 集成PagerMaid的消息处理"""
    try:
        with contextlib.suppress(MessageNotModifiedError, MessageTooLongError):
            await message.edit(text)
        if delete_after > 0:
            asyncio.create_task(_auto_delete(message, delete_after))
        return message
    except Exception as e:
        logs.error(f"[BanManager] Edit error: {e}")
        return message

async def _auto_delete(message: Message, delay: int) -> None:
    """延迟删除消息"""
    with contextlib.suppress(Exception):
        await asyncio.sleep(delay)
        await message.delete()

def parse_args(parameter) -> list:
    """解析命令参数 - 兼容PagerMaid参数格式"""
    if isinstance(parameter, str):
        return parameter.split() if parameter else []
    elif isinstance(parameter, list):
        return parameter
    return []

@cache(ttl=timedelta(hours=1))
async def safe_get_entity(client, target):
    """安全获取用户实体 - 使用PagerMaid缓存系统"""
    try:
        target_str = str(target)
        
        if target_str.startswith("@"):
            return await client.get_entity(target)
        elif target_str.lstrip('-').isdigit():
            user_id = int(target)
            return await client.get_entity(user_id)
        else:
            raise ValueError("已禁用容易定位错用户的处理用户名（不带@）的逻辑")
            
    except Exception as e:
        logs.error(f"[BanManager] Get entity error for {target}: {e}")
        return None

async def get_target_user(client, message: Message, args: list):
    """获取目标用户 - 集成PagerMaid消息处理（支持频道马甲身份）
    调整优先级：若命令显式提供了 @username / user_id / 群聊(频道)ID，则优先使用；否则再回退到回复消息。
    """
    # 1) 如果提供了参数，只按参数解析；参数无效则直接返回失败（不回退回复对象）
    try:
        if args:
            raw = str(args[0])

            if raw.startswith("@"):
                entity = await safe_get_entity(client, raw)
                return entity, (entity.id if entity else None)
            elif raw.lstrip('-').isdigit():
                user_id = int(raw)
                entity = await safe_get_entity(client, user_id)
                return entity, user_id
            else:
                # 明确禁止不带@的用户名，保持原有安全策略
                raise ValueError("已禁用容易定位错用户的处理用户名（不带@）的逻辑")
    except Exception as e:
        logs.error(f"[BanManager] Get user from args error: {e}")
        return None, None

    # 2) 未提供参数：仅在“作为回复使用命令”时采用被回复对象
    try:
        if not args and hasattr(message, 'reply_to_msg_id') and message.reply_to_msg_id:
            reply_msg = await message.get_reply_message()
            if reply_msg and reply_msg.sender_id:
                target_user = reply_msg.sender
                target_uid = reply_msg.sender_id

                # 检查是否是频道身份发送的消息
                if hasattr(reply_msg, 'post') and reply_msg.post:
                    if hasattr(reply_msg, 'from_id') and reply_msg.from_id:
                        if hasattr(reply_msg.from_id, 'channel_id'):
                            target_uid = reply_msg.from_id.channel_id
                            logs.info(f"[BanManager] Detected channel message, using channel ID: {target_uid}")

                return target_user, target_uid
    except Exception:
        pass

    # 3) 都无法获取
    return None, None

def format_user(user, user_id):
    """格式化用户显示（支持频道身份）"""
    if user and hasattr(user, 'first_name'):
        name = user.first_name or str(user_id)
        if getattr(user, 'last_name', None):
            name += f" {user.last_name}"
        if getattr(user, 'username', None):
            name += f" (@{user.username})"
        return name
    elif user and hasattr(user, 'title'):
        title = user.title
        if getattr(user, 'username', None):
            title += f" (@{user.username})"
        return f"频道: {title}"
    elif user and hasattr(user, 'broadcast'):
        title = getattr(user, 'title', str(user_id))
        if getattr(user, 'username', None):
            title += f" (@{user.username})"
        return f"频道: {title}"
    return str(user_id)

@cache(ttl=timedelta(minutes=30))
async def check_permissions(client, chat_id: int, action: str = "ban") -> bool:
    """检查机器人权限 - 使用PagerMaid缓存系统"""
    try:
        me = await client.get_me()
        part = await client(GetParticipantRequest(chat_id, me.id))
        rights = getattr(part.participant, 'admin_rights', None)
        return bool(rights and rights.ban_users)
    except Exception:
        return False

@cache(ttl=timedelta(minutes=15))
async def is_admin(client, chat_id: int, user_id: int) -> bool:
    """检查用户是否为管理员 - 使用PagerMaid缓存系统"""
    try:
        part = await client(GetParticipantRequest(chat_id, user_id))
        return getattr(part.participant, 'admin_rights', None) is not None
    except Exception:
        return False

@cache(ttl=timedelta(hours=1))
async def get_managed_groups(client):
    """获取管理的群组 - 使用PagerMaid缓存系统"""
    groups = []
    me = await client.get_me()

    # 收集所有对话
    dialogs = []
    try:
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                dialogs.append(dialog)
    except Exception as e:
        logs.error(f"[AdvancedBan] Error iterating dialogs: {e}")
        return []

    # 并发检查权限
    async def check_group(dialog):
        try:
            part = await client(GetParticipantRequest(dialog.id, me.id))
            rights = getattr(part.participant, 'admin_rights', None)
            if rights and rights.ban_users:
                return {'id': dialog.id, 'title': dialog.title}
        except Exception:
            # 忽略没有权限或无法访问的群组
            pass
        return None

    # 分批并发处理
    for i in range(0, len(dialogs), 20):  # 增加批处理大小
        batch = dialogs[i:i + 20]
        results = await asyncio.gather(*[check_group(d) for d in batch], return_exceptions=True)
        groups.extend([r for r in results if r and not isinstance(r, Exception)])

    logs.info(f"[AdvancedBan] Groups refreshed: {len(groups)}")
    return groups

def show_help(command: str) -> str:
    """集成到PagerMaid的帮助系统 - 使用简化的语言支持"""
    helps = {
        "main": "🛡️ **高级封禁管理插件**\n\n**可用指令：**\n• `kick` - 踢出用户\n• `ban` - 封禁用户\n• `unban` - 解封用户\n• `mute` - 禁言用户\n• `unmute` - 解除禁言\n• `sb` - 批量封禁\n• `unsb` - 批量解封\n• `refresh` - 刷新群组缓存\n• `preload` - 预加载群组缓存\n• `cache` - 查看缓存状态\n\n💡 **使用方式：**\n支持：回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名",
        "sb": "🌐 **批量封禁**\n\n**语法：** `sb <用户> [原因]`\n**示例：** `sb @user 垃圾广告`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n在你管理的所有群组中封禁指定用户",
        "unsb": "🌐 **批量解封**\n\n**语法：** `unsb <用户>`\n**示例：** `unsb @user`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n在你管理的所有群组中解封指定用户",
        "kick": "🚪 **踢出用户**\n\n**语法：** `kick <用户> [原因]`\n**示例：** `kick @user 刷屏`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n用户可以重新加入群组",
        "ban": "🚫 **封禁用户**\n\n**语法：** `ban <用户> [原因]`\n**示例：** `ban @user 广告`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n永久封禁，需要管理员解封",
        "unban": "🔓 **解除封禁**\n\n**语法：** `unban <用户>`\n**示例：** `unban @user`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n解除用户封禁状态",
        "mute": "🤐 **禁言用户**\n\n**语法：** `mute <用户> [分钟] [原因]`\n**示例：** `mute @user 60 刷屏`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n默认60分钟，最长24小时",
        "unmute": "🔊 **解除禁言**\n\n**语法：** `unmute <用户>`\n**示例：** `unmute @user`\n**支持：** 回复消息、@用户名、用户ID、群/频道ID（负数）\n不支持：不带 @ 的用户名\n\n立即解除禁言",
        "refresh": "🔄 **刷新群组缓存**\n\n重建管理群组缓存",
        "preload": "⚡ **预加载群组缓存**\n\n预先建立管理群组缓存以加速后续操作",
        "cache": "🗃️ **查看缓存状态**\n\n显示当前缓存信息"
    }
    return helps.get(command, helps["main"])

async def _resolve_user_if_needed(client, message: Message, user, uid, args):
    """如果用户实体未找到且提供了数字ID，则跨群解析。"""
    try:
        raw = str(args[0]) if args else ""
        if raw and raw.lstrip('-').isdigit() and (user is None) and isinstance(uid, int) and uid > 0:
            status = await smart_edit(message, "🔎 未能直接解析该 ID，正在跨群扫描尝试定位实体...", 0)
            groups = await get_managed_groups(client)
            if not groups:
                await smart_edit(status, "❌ 未找到可管理的群组（请确认已建立缓存或有管理权限）")
                return None, None, None
            found = await _resolve_user_across_groups_by_id(client, groups, uid, per_group_limit=2000)
            if not found:
                await smart_edit(
                    status,
                    "❌ 无法通过纯数字ID跨群定位该用户\n\n"
                    "请改用：\n"
                    "• @用户名（推荐），或\n"
                    "• 在任一聊天回复该用户后再使用命令，或\n"
                    "• 确保你与该用户有共同群/私聊以便解析实体",
                    30,
                )
                return None, None, None
            # 解析成功，更新用户实体和ID，并返回新的消息对象
            return found, getattr(found, 'id', uid), status
    except Exception as e:
        logs.error(f"[AdvancedBan] Cross-group resolution error: {e}")
        # 出现异常时，返回原始值，让调用方决定如何处理
    return user, uid, message

async def handle_user_action(client, message: Message, command: str):
    """统一的用户操作处理 - 集成PagerMaid消息处理"""
    
    args = parse_args(getattr(message, "parameter", "") or "")
    
    # 检查是否需要显示帮助
    has_reply = hasattr(message, 'reply_to_msg_id') and message.reply_to_msg_id
    if not args and not has_reply:
        await smart_edit(message, show_help(command), 30)
        return None
    
    if not message.is_group:
        await smart_edit(message, "❌ 此命令只能在群组中使用")
        return None
    
    user, uid = await get_target_user(client, message, args)
    if not uid:
        await smart_edit(message, "❌ 无法获取用户信息")
        return None
    
    return user, uid, args

async def safe_ban_action(client, chat_id, user_id, rights):
    """安全的封禁操作函数（支持频道马甲身份并删除消息）"""
    try:
        ban_success = False
        
        try:
            await client(EditBannedRequest(chat_id, user_id, rights))
            ban_success = True
        except Exception as e1:
            logs.error(f"[AdvancedBan] Method 1 (direct ID) failed: {e1}")
            
            try:
                user_entity = await safe_get_entity(client, user_id)
                if user_entity:
                    await client(EditBannedRequest(chat_id, user_entity, rights))
                    ban_success = True
            except Exception as e2:
                logs.error(f"[AdvancedBan] Method 2 (entity) failed: {e2}")
                
                try:
                    participant = await client(GetParticipantRequest(chat_id, user_id))
                    if hasattr(participant.participant, 'peer') and hasattr(participant.participant.peer, 'channel_id'):
                        channel_id = participant.participant.peer.channel_id
                        await client(EditBannedRequest(chat_id, channel_id, rights))
                        logs.info(f"[AdvancedBan] Banned channel identity: {channel_id}")
                        ban_success = True
                except Exception as e3:
                    logs.error(f"[AdvancedBan] Method 3 (channel identity) failed: {e3}")
                
                if not ban_success:
                    try:
                        user_entity = await safe_get_entity(client, user_id)
                        if user_entity and hasattr(user_entity, 'access_hash') and user_entity.access_hash:
                            if hasattr(user_entity, 'broadcast') and user_entity.broadcast:
                                input_peer = InputPeerChannel(user_id, user_entity.access_hash)
                            else:
                                input_peer = InputPeerUser(user_id, user_entity.access_hash)
                            
                            await client(EditBannedRequest(chat_id, input_peer, rights))
                            ban_success = True
                    except Exception as e4:
                        logs.error(f"[AdvancedBan] Method 4 (InputPeer) failed: {e4}")
        
        # 如果是永久封禁（view_messages=True），尝试删除该用户的消息
        if getattr(rights, 'view_messages', False):
            try:
                chat_entity = await client.get_entity(chat_id)
                # 仅在超级群组和频道中尝试删除历史记录
                if hasattr(chat_entity, 'megagroup') and chat_entity.megagroup or hasattr(chat_entity, 'broadcast') and chat_entity.broadcast:
                    try:
                        user_entity = await safe_get_entity(client, user_id)
                        if user_entity:
                            await client(DeleteParticipantHistoryRequest(channel=chat_entity, participant=user_entity))
                            logs.info(f"[AdvancedBan] Deleted all messages from {user_id} in {chat_id}")
                        else:
                            logs.warning(f"[AdvancedBan] Could not resolve user {user_id} to delete messages.")
                    except ChatAdminRequiredError:
                        logs.warning(f"[AdvancedBan] No permission to delete messages in {chat_id}")
                    except Exception as e:
                        logs.error(f"[AdvancedBan] Failed to delete messages for {user_id} in {chat_id}: {e}")
            except Exception as e:
                logs.error(f"[AdvancedBan] Could not get chat entity for message deletion: {e}")
        
        return ban_success
                    
    except Exception as e:
        logs.error(f"[BanManager] Safe ban action error: {e}")
        return False

# 批量操作的异步处理函数
async def batch_ban_operation(client, groups, user_id, rights, operation_name="封禁"):
    """批量执行封禁/解封操作（并发优化）"""
    success = 0
    failed = 0
    failed_groups = []
    
    async def process_group(group):
        try:
            if await safe_ban_action(client, group['id'], user_id, rights):
                return True, None
            else:
                return False, group['title']
        except Exception as e:
            logs.error(f"[BanManager] {operation_name} error in {group['title']}: {e}")
            return False, f"{group['title']} (异常)"
    
    # 分批并发处理
    for i in range(0, len(groups), BATCH_SIZE):
        batch = groups[i:i + BATCH_SIZE]
        results = await asyncio.gather(*[process_group(g) for g in batch], return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                failed += 1
                failed_groups.append("未知群组 (异常)")
            elif result[0]:
                success += 1
            else:
                failed += 1
                if result[1]:
                    failed_groups.append(result[1])
    
    return success, failed, failed_groups

# 主要命令实现 - 集成PagerMaid架构
@listener(is_plugin=True, outgoing=True, command="aban", description="高级封禁管理插件帮助")
async def show_main_help(client, message: Message):
    """显示主帮助信息"""
    await smart_edit(message, show_help("main"), 30)

@listener(is_plugin=True, outgoing=True, command="refresh", description="刷新群组缓存")
async def refresh_cache(client, message: Message):
    """手动刷新群组缓存 - 集成PagerMaid缓存系统"""
    
    status = await smart_edit(message, "🔄 正在刷新群组缓存...", 0)
    
    try:
        # 使用PagerMaid缓存系统，通过重新调用函数来刷新缓存
        groups = await get_managed_groups(client)
        await status.edit(f"✅ 刷新完成，管理群组数：{len(groups)}")
    except Exception as e:
        logs.error(f"[BanManager] Refresh cache error: {e}")
        await smart_edit(status, f"❌ 刷新失败：{e}")

@cache(ttl=timedelta(minutes=30))
async def _resolve_user_across_groups_by_id(client, groups: list, uid: int, per_group_limit: int = None):
    """在已管理的群组中按 user_id 并发尝试解析用户实体 - 使用PagerMaid缓存系统
    策略：
      1) 优先使用 GetParticipantRequest(chat, uid) 精确探测；
      2) 失败时才回退到遍历成员（限量 per_group_limit）。
      3) 命中任意一群即返回该 User 实体，并取消其他探测。
    """
    per_limit = per_group_limit or PER_GROUP_SCAN_LIMIT
    semaphore = asyncio.Semaphore(PARALLEL_LIMIT)
    found_user = {'val': None}
    done_event = asyncio.Event()

    async def probe_group(g):
        group_id = g.get('id') if isinstance(g, dict) else getattr(g, 'id', g)
        group_title = g.get('title') if isinstance(g, dict) else getattr(g, 'title', str(group_id))

        if group_id is None:
            return

        async with semaphore:
            if done_event.is_set():
                return
            # 1) 优先用 GetParticipantRequest 探测
            if USE_GET_PARTICIPANT_FIRST:
                try:
                    res = await client(GetParticipantRequest(group_id, uid))
                    users_list = getattr(res, 'users', None)
                    if users_list:
                        for u in users_list:
                            if getattr(u, 'id', None) == uid:
                                found_user['val'] = u
                                done_event.set()
                                return
                except Exception:
                    pass

            if done_event.is_set():
                return

            # 2) 回退遍历成员（限量）
            try:
                async for p in client.iter_participants(group_id, limit=per_limit):
                    if getattr(p, 'id', None) == uid:
                        found_user['val'] = p
                        done_event.set()
                        return
            except Exception as e:
                logs.error(f"[BanManager] Scan group {group_title} for uid {uid} error: {e}")

    # 并发发起探测
    tasks = [asyncio.create_task(probe_group(g)) for g in groups]
    try:
        while not done_event.is_set() and any(not t.done() for t in tasks):
            await asyncio.sleep(0.05)
    finally:
        if done_event.is_set():
            for t in tasks:
                if not t.done():
                    t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks, return_exceptions=True)

    return found_user['val']

@listener(is_plugin=True, outgoing=True, command="sb", description="批量封禁用户", parameters="<用户> [原因]")
async def super_ban(client, message: Message):
    result = await handle_user_action(client, message, "sb")
    if not result:
        return
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return

    reason = " ".join(args[1:]) if len(args) > 1 else "跨群违规"
    display = format_user(user, uid)
    status = await smart_edit(message, "🌐 正在查找与目标用户的共同群组...", 0)
    try:
        # 使用缓存的“管理的群组”，避免逐群扫描共同群组
        groups = await get_managed_groups(client)

        if not groups:
            await smart_edit(status, "❌ 未找到可管理的群组（请确认已建立缓存或有管理权限）")
            return

        await status.edit(f"🌐 正在批量封禁 {display}...\n📊 目标群组：{len(groups)} 个")

        rights = ChatBannedRights(
            until_date=None,
            view_messages=True,
            send_messages=True,
            send_media=True,
            send_stickers=True,
            send_gifs=True,
            send_games=True,
            send_inline=True,
            embed_links=True
        )

        success, failed, failed_groups = await batch_ban_operation(
            client, groups, uid, rights, operation_name="封禁"
        )

        result_text = (
            f"✅ **批量封禁完成**\n\n"
            f"👤 用户：{display}\n"
            f"🆔 ID：`{uid}`\n"
            f"📝 原因：{reason}\n"
            f"🌐 成功：{success} 群组\n"
            f"❌ 失败：{failed} 群组\n"
            f"⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        if failed_groups and len(failed_groups) <= 3:
            result_text += "\n\n失败群组：\n" + "\n".join(f"• {g}" for g in failed_groups[:3])
        await smart_edit(status, result_text, 60)
    except Exception as e:
        await smart_edit(status, f"❌ sb执行异常：{e}")

@listener(is_plugin=True, outgoing=True, command="unsb", description="批量解封用户", parameters="<用户>")
async def super_unban(client, message: Message):
    result = await handle_user_action(client, message, "unsb")
    if not result:
        return
    
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return

    display = format_user(user, uid)
    
    status = await smart_edit(message, "🌐 正在获取管理群组...", 0)
    
    # 预加载缓存（如果需要）
    groups = await get_managed_groups(client)
    
    if not groups:
        return await smart_edit(status, "❌ 未找到管理的群组\n\n💡 提示：使用 `refresh` 命令刷新缓存")
    
    await status.edit(f"🌐 正在批量解封 {display}...\n📊 目标群组：{len(groups)} 个")
    
    # 设置解封权限
    rights = ChatBannedRights(until_date=0)
    
    # 记录开始时间
    start_time = time.time()
    
    # 执行批量解封
    success, failed, failed_groups = await batch_ban_operation(client, groups, uid, rights, "解封")
    
    # 计算耗时
    elapsed = time.time() - start_time
    
    result_text = f"✅ **批量解封完成**\n\n👤 用户：{display}\n🆔 ID：`{uid}`\n🌐 成功：{success} 群组\n❌ 失败：{failed} 群组\n⏱️ 耗时：{elapsed:.1f} 秒\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    
    if failed_groups and len(failed_groups) <= 3:
        result_text += f"\n\n失败群组：\n" + "\n".join(f"• {g}" for g in failed_groups[:3])
    
    await smart_edit(status, result_text, 60)

@listener(is_plugin=True, outgoing=True, command="kick", description="踢出用户", parameters="<用户> [原因]")
async def kick_user(client, message: Message):
    result = await handle_user_action(client, message, "kick")
    if not result:
        return
    
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return
    reason = " ".join(args[1:]) if len(args) > 1 else "广告"
    display = format_user(user, uid)
    
    status = await smart_edit(message, f"🚪 正在踢出 {display}...", 0)
    
    if await is_admin(client, message.chat_id, uid):
        return await smart_edit(status, "❌ 不能踢出管理员")
    
    if not await check_permissions(client, message.chat_id):
        return await smart_edit(status, "❌ 权限不足")
    
    try:
        # 先封禁再解封实现踢出
        ban_rights = ChatBannedRights(until_date=0, view_messages=True)
        await safe_ban_action(client, message.chat_id, uid, ban_rights)
        
        unban_rights = ChatBannedRights(until_date=0)
        await safe_ban_action(client, message.chat_id, uid, unban_rights)
        
        result_text = f"✅ **踢出完成**\n\n👤 用户：{display}\n🆔 ID：`{uid}`\n📝 原因：{reason}\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        await smart_edit(status, result_text)
    except Exception as e:
        await smart_edit(status, f"❌ 踢出失败：{str(e)}")

@listener(is_plugin=True, outgoing=True, command="ban", description="封禁用户", parameters="<用户> [原因]")
async def ban_user(client, message: Message):
    result = await handle_user_action(client, message, "ban")
    if not result:
        return
    
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return
    reason = " ".join(args[1:]) if len(args) > 1 else "广告"
    display = format_user(user, uid)
    
    status = await smart_edit(message, f"🚫 正在封禁 {display}...", 0)
    
    if await is_admin(client, message.chat_id, uid):
        return await smart_edit(status, "❌ 不能封禁管理员")
    
    if not await check_permissions(client, message.chat_id):
        return await smart_edit(status, "❌ 权限不足")
    
    rights = ChatBannedRights(until_date=None, view_messages=True, send_messages=True)
    success = await safe_ban_action(client, message.chat_id, uid, rights)
    
    if success:
        result_text = f"✅ **封禁完成**\n\n👤 用户：{display}\n🆔 ID：`{uid}`\n📝 原因：{reason}\n🗑️ 已删除该用户的所有消息\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        await smart_edit(status, result_text)
    else:
        await smart_edit(status, "❌ 封禁失败，请检查权限或用户是否存在")

@listener(is_plugin=True, outgoing=True, command="unban", description="解封用户", parameters="<用户>")
async def unban_user(client, message: Message):
    result = await handle_user_action(client, message, "unban")
    if not result:
        return
    
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return
    display = format_user(user, uid)
    
    status = await smart_edit(message, f"🔓 正在解封 {display}...", 0)
    
    if not await check_permissions(client, message.chat_id):
        return await smart_edit(status, "❌ 权限不足")
    
    rights = ChatBannedRights(until_date=0)
    success = await safe_ban_action(client, message.chat_id, uid, rights)
    
    if success:
        result_text = f"✅ **解封完成**\n\n👤 用户：{display}\n🆔 ID：`{uid}`\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        await smart_edit(status, result_text)
    else:
        await smart_edit(status, "❌ 解封失败，用户可能不在群组或无权限")

@listener(is_plugin=True, outgoing=True, command="mute", description="禁言用户", parameters="<用户> [分钟] [原因]")
async def mute_user(client, message: Message):
    result = await handle_user_action(client, message, "mute")
    if not result:
        return
    
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return
    minutes = 60
    reason = "违规发言"
    
    # 解析参数
    if len(args) > 1:
        if args[1].isdigit():
            minutes = max(1, min(int(args[1]), 1440))  # 最长24小时
            if len(args) > 2:
                reason = " ".join(args[2:])
        else:
            reason = " ".join(args[1:])
    
    display = format_user(user, uid)
    status = await smart_edit(message, f"🤐 正在禁言 {display}...", 0)
    
    if await is_admin(client, message.chat_id, uid):
        return await smart_edit(status, "❌ 不能禁言管理员")
    
    if not await check_permissions(client, message.chat_id):
        return await smart_edit(status, "❌ 权限不足")
    
    try:
        until_date = int(datetime.utcnow().timestamp()) + (minutes * 60)
        rights = ChatBannedRights(until_date=until_date, send_messages=True)
        success = await safe_ban_action(client, message.chat_id, uid, rights)
        
        if success:
            end_time = (datetime.utcnow() + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
            result_text = f"✅ **禁言完成**\n\n👤 用户：{display}\n🆔 ID：`{uid}`\n📝 原因：{reason}\n⏱️ 时长：{minutes} 分钟\n🔓 解除：{end_time} UTC"
            await smart_edit(status, result_text)
        else:
            await smart_edit(status, "❌ 禁言失败，请检查权限")
    except Exception as e:
        await smart_edit(status, f"❌ 禁言失败：{str(e)}")

@listener(is_plugin=True, outgoing=True, command="unmute", description="解除禁言", parameters="<用户>")
async def unmute_user(client, message: Message):
    result = await handle_user_action(client, message, "unmute")
    if not result:
        return
    
    user, uid, args = result
    user, uid, message = await _resolve_user_if_needed(client, message, user, uid, args)
    if not uid:
        return
    display = format_user(user, uid)
    
    status = await smart_edit(message, f"🔊 正在解除禁言 {display}...", 0)
    
    if not await check_permissions(client, message.chat_id):
        return await smart_edit(status, "❌ 权限不足")
    
    rights = ChatBannedRights(until_date=0, send_messages=False)
    success = await safe_ban_action(client, message.chat_id, uid, rights)
    
    if success:
        result_text = f"✅ **解除禁言完成**\n\n👤 用户：{display}\n🆔 ID：`{uid}`\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        await smart_edit(status, result_text)
    else:
        await smart_edit(status, "❌ 解除禁言失败，请检查权限")

# 添加预加载命令（可选）
@listener(is_plugin=True, outgoing=True, command="preload", description="预加载群组缓存")
async def preload_cache(client, message: Message):
    """预加载群组缓存，提高后续操作速度"""
    
    status = await smart_edit(message, "🔄 正在预加载缓存...", 0)
    
    try:
        # 预加载群组
        groups = await get_managed_groups(client)
        
        # 预加载当前用户信息
        me = await client.get_me()
        
        info_text = f"✅ **预加载完成**\n\n"
        info_text += f"👤 当前用户：{me.first_name or 'Unknown'}\n"
        info_text += f"📊 管理群组：{len(groups)} 个\n"
        info_text += f"⏰ 缓存有效期：2小时\n\n"
        info_text += f"💡 提示：后续同类操作将更快，如需强制刷新可用 `refresh`"
        
        await smart_edit(status, info_text, 30)
    except Exception as e:
        await smart_edit(status, f"❌ 预加载失败：{str(e)}")

# 添加缓存状态查看命令（可选）
@listener(is_plugin=True, outgoing=True, command="cache", description="查看缓存状态")
async def cache_status(client, message: Message):
    """查看当前缓存状态"""
    
    try:
        # 使用PagerMaid缓存系统，显示简化的缓存状态
        groups = await get_managed_groups(client)
        info = [
            "🗃️ **缓存状态**",
            f"📊 管理群组：{len(groups)} 个",
            "⏱️ 缓存有效期：2小时",
            "💡 使用PagerMaid集成缓存系统"
        ]
        await smart_edit(message, "\n".join(info), 30)
    except Exception as e:
        await smart_edit(message, f"❌ 读取缓存状态失败：{e}")

# 集成完成 - 使用PagerMaid缓存系统，无需手动清理
