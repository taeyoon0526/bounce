from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord import ui
from discord.ext import tasks
from discord.errors import Forbidden, HTTPException, NotFound
from discord.utils import format_dt
from redbot.core import Config, commands
from redbot.core.bot import Red


DURATION_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[mhd])$")

ACTION_STATUS_ID = 9101


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_duration(value: str) -> Optional[int]:
    match = DURATION_RE.match(value.lower().strip())
    if not match:
        return None
    amount = int(match.group("value"))
    unit = match.group("unit")
    if amount <= 0:
        return None
    if unit == "m":
        return amount * 60
    if unit == "h":
        return amount * 60 * 60
    if unit == "d":
        return amount * 60 * 60 * 24
    return None


def _format_duration(seconds: int) -> str:
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _format_minutes(seconds: int) -> str:
    minutes = max(1, int(round(seconds / 60)))
    return str(minutes)


def _format_days(seconds: int) -> str:
    days = max(1, int(round(seconds / 86400)))
    return str(days)


def _text_view(text: str) -> ui.LayoutView:
    view = ui.LayoutView()
    box = ui.Container(accent_color=discord.Color.blurple().value)
    box.add_item(ui.TextDisplay(text))
    view.add_item(box)
    return view


async def _get_guild_invite_link(guild: Optional[discord.Guild]) -> Optional[str]:
    if not guild:
        return None
    try:
        vanity = await guild.vanity_invite()
        if vanity and vanity.url:
            return vanity.url
    except (Forbidden, HTTPException, AttributeError):
        pass

    channel: Optional[discord.TextChannel] = None
    bot_member = guild.me
    if isinstance(guild.system_channel, discord.TextChannel):
        if not bot_member or guild.system_channel.permissions_for(bot_member).create_instant_invite:
            channel = guild.system_channel
    if not channel:
        for candidate in guild.text_channels:
            if not bot_member or candidate.permissions_for(bot_member).create_instant_invite:
                channel = candidate
                break
    if not channel:
        return None
    try:
        invite = await channel.create_invite(max_age=0, max_uses=0, temporary=False, unique=False)
        return invite.url
    except (Forbidden, HTTPException):
        return None


def _dm_layout(
    guild_name: str,
    contacts_text: str,
    bounce_count: int,
    ban_seconds: Optional[int],
    unban_time: Optional[datetime],
    permban: bool,
    invite_url: Optional[str],
) -> ui.LayoutView:
    view = ui.LayoutView()
    header_box = ui.Container(accent_color=discord.Color.red().value if permban else discord.Color.orange().value)
    if permban:
        header_box.add_item(ui.TextDisplay("## ⛔ 영구 밴 안내"))
        header_box.add_item(ui.TextDisplay(f"안녕하세요. {guild_name} 운영팀입니다."))
        header_box.add_item(ui.Separator(visible=True))
        info_box = header_box
        info_box.add_item(ui.TextDisplay(
            "**사유**\n"
            "들낙이 누적 3회 이상 확인되어 영구 밴이 적용되었습니다."
        ))
        info_box.add_item(ui.TextDisplay(f"**누적 횟수**\n{bounce_count}회"))
        info_box.add_item(ui.TextDisplay(
            "**문의/재검토**\n"
            "이 조치에 대해 문의가 필요하시면 아래 담당자에게 DM으로 연락해 주세요.\n"
            "담당자 목록(일부):\n"
            f"{contacts_text}"
        ))
        if invite_url:
            info_box.add_item(ui.TextDisplay(f"**서버 초대 링크**\n{invite_url}"))
        info_box.add_item(ui.Separator(spacing=discord.SeparatorSpacing.small))
        info_box.add_item(ui.TextDisplay("문의 시 상황을 간략히 알려주시면 빠르게 확인하겠습니다."))
        view.add_item(info_box)
        return view

    header_box.add_item(ui.TextDisplay("## ⚠️ 임시 밴 안내"))
    header_box.add_item(ui.TextDisplay(f"안녕하세요. {guild_name} 운영팀입니다."))
    header_box.add_item(ui.Separator(visible=True))
    info_box = header_box
    info_box.add_item(ui.TextDisplay(
        "**사유**\n"
        "단시간 입장/퇴장 기록이 확인되어 자동 임시 밴이 적용되었습니다."
    ))
    if ban_seconds is not None and unban_time is not None:
        info_box.add_item(ui.TextDisplay(
            "**밴 정보**\n"
            f"기간: {_format_duration(ban_seconds)}\n"
            f"해제 예정: {format_dt(unban_time)}"
        ))
    info_box.add_item(ui.TextDisplay(f"**누적 횟수**\n{bounce_count}회"))
    info_box.add_item(ui.TextDisplay(
        "**문의/재검토**\n"
        "문의가 필요하시면 아래 담당자에게 DM으로 연락해 주세요.\n"
        "담당자 목록(일부):\n"
        f"{contacts_text}"
    ))
    if invite_url:
        info_box.add_item(ui.TextDisplay(f"**서버 초대 링크**\n{invite_url}"))
    info_box.add_item(ui.Separator(spacing=discord.SeparatorSpacing.small))
    info_box.add_item(ui.TextDisplay("문의 시 상황을 간략히 알려주시면 빠르게 확인하겠습니다."))
    view.add_item(info_box)
    return view


class LogActionButton(ui.Button):
    def __init__(
        self,
        cog: "Bounce",
        action: str,
        guild_id: int,
        user_id: int,
        disabled: bool = False,
    ) -> None:
        label = "영구밴" if action == "permban" else "밴해제"
        style = discord.ButtonStyle.danger if action == "permban" else discord.ButtonStyle.secondary
        custom_id = f"bounce:{action}:{guild_id}:{user_id}"
        super().__init__(label=label, style=style, custom_id=custom_id, disabled=disabled)
        self.cog = cog
        self.action = action
        self.guild_id = guild_id
        self.user_id = user_id

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.cog._handle_log_action(
            interaction=interaction,
            action=self.action,
            guild_id=self.guild_id,
            user_id=self.user_id,
            source_view=self.view,
        )


class LogActionLayout(ui.LayoutView):
    def __init__(
        self,
        cog: "Bounce",
        guild_id: int,
        user_id: int,
        payload: Dict[str, object],
        disabled: bool = False,
        show_permban: bool = True,
    ) -> None:
        super().__init__(timeout=None)
        permban = bool(payload.get("permban"))
        title = "들낙 감지 - 영구밴" if permban else "들낙 감지 - 임시밴"
        info_box = ui.Container(accent_color=discord.Color.red().value)
        info_box.add_item(ui.TextDisplay(f"## {title}"))
        info_box.add_item(ui.Separator(visible=True))
        member_tag = payload.get("member_tag", "알 수 없음")
        join_ts = float(payload.get("join_time", 0.0))
        leave_ts = float(payload.get("leave_time", 0.0))
        elapsed_seconds = float(payload.get("elapsed_seconds", 0.0))
        dm_result = payload.get("dm_result", "알 수 없음")
        bounce_count = int(payload.get("bounce_count", 0))
        ban_seconds = payload.get("ban_seconds")
        unban_ts = payload.get("unban_time")

        join_time = datetime.fromtimestamp(join_ts, timezone.utc) if join_ts else _utcnow()
        leave_time = datetime.fromtimestamp(leave_ts, timezone.utc) if leave_ts else _utcnow()

        info_box.add_item(ui.TextDisplay(
            f"**유저**\n{member_tag}\n{user_id}\n<@{user_id}>"
        ))
        info_box.add_item(ui.TextDisplay(
            "**시간**\n"
            f"join: {format_dt(join_time)}\n"
            f"leave: {format_dt(leave_time)}\n"
            f"경과: {elapsed_seconds:.1f}초"
        ))
        info_box.add_item(ui.TextDisplay(f"**DM**\n{dm_result}"))
        action_status = payload.get("action_status")
        if action_status:
            info_box.add_item(ui.TextDisplay(str(action_status), id=ACTION_STATUS_ID))
        else:
            info_box.add_item(ui.TextDisplay("**조치**\n대기 중", id=ACTION_STATUS_ID))
        info_box.add_item(ui.TextDisplay(f"**들낙 누적**\n{bounce_count}회"))
        if permban:
            info_box.add_item(ui.TextDisplay("**밴**\n영구 밴"))
        elif ban_seconds is not None and unban_ts is not None:
            unban_time = datetime.fromtimestamp(float(unban_ts), timezone.utc)
            info_box.add_item(ui.TextDisplay(
                "**밴**\n"
                f"기간: {_format_duration(int(ban_seconds))}\n"
                f"해제 예정: {format_dt(unban_time)}"
            ))
        info_box.add_item(ui.Separator(spacing=discord.SeparatorSpacing.large))

        actions = ui.ActionRow()
        if show_permban:
            actions.add_item(LogActionButton(cog, "permban", guild_id, user_id, disabled=disabled))
        actions.add_item(LogActionButton(cog, "unban", guild_id, user_id, disabled=disabled))
        info_box.add_item(actions)
        self.add_item(info_box)


class Bounce(commands.Cog):
    """Detects quick join/leave and applies a temporary ban."""

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(self, identifier=9812734992, force_registration=True)
        default_guild = {
            "enabled": False,
            "window_seconds": 60,
            "ban_duration_seconds": 86400,
            "role_ids": [],
            "log_channel_id": None,
            "max_contacts": 25,
            "include_bots": False,
            "welcome_enabled": False,
            "repeat_detection": {
                "enabled": False,
                "window_minutes": 5,
                "threshold": 3,
            },
            "tempbans": [],
            "log_actions": [],
            "bounce_counts": {},
        }
        self.config.register_guild(**default_guild)
        self.join_cache: Dict[int, Dict[int, datetime]] = {}
        self.unban_task = self._unban_loop
        self.cleanup_task = self._cleanup_loop
        self.unban_task.start()
        self.cleanup_task.start()

    async def cog_load(self) -> None:
        await self._restore_log_action_views()

    def cog_unload(self) -> None:
        self.unban_task.cancel()
        self.cleanup_task.cancel()

    async def _get_log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        channel_id = await self.config.guild(guild).log_channel_id()
        if not channel_id:
            return None
        channel = guild.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            return channel
        return None

    def _should_ignore_member(self, member: discord.Member, include_bots: bool) -> bool:
        return member.bot and not include_bots

    async def _build_contacts(self, guild: discord.Guild) -> Tuple[str, int]:
        role_ids = await self.config.guild(guild).role_ids()
        max_contacts = await self.config.guild(guild).max_contacts()
        members: List[discord.Member] = []
        seen = set()
        for role_id in role_ids:
            role = guild.get_role(role_id)
            if not role:
                continue
            for member in role.members:
                if member.id in seen:
                    continue
                seen.add(member.id)
                members.append(member)
        lines = []
        for member in members[:max_contacts]:
            lines.append(f"- `{member.id}` (<@{member.id}>)")
        remaining = max(0, len(members) - max_contacts)
        if not lines:
            return "담당자 목록이 비어있습니다.", 0
        text = "\n".join(lines)
        if remaining:
            text = f"{text}\n- 외 {remaining}명"
        return text, len(members)

    async def _send_dm(
        self,
        member: discord.Member,
        contacts_text: str,
        bounce_count: int,
        ban_seconds: Optional[int] = None,
        unban_time: Optional[datetime] = None,
        permban: bool = False,
    ) -> Tuple[bool, str]:
        guild_name = member.guild.name if member.guild else "해당 서버"
        try:
            invite_url = await _get_guild_invite_link(member.guild)
            layout = _dm_layout(
                guild_name=guild_name,
                contacts_text=contacts_text,
                bounce_count=bounce_count,
                ban_seconds=ban_seconds,
                unban_time=unban_time,
                permban=permban,
                invite_url=invite_url,
            )
            await member.send(view=layout)
            return True, "성공"
        except (Forbidden, HTTPException) as exc:
            return False, f"실패: {exc}"

    async def _log_action(
        self,
        guild: discord.Guild,
        member_id: int,
        member_tag: str,
        join_time: datetime,
        leave_time: datetime,
        elapsed_seconds: float,
        dm_result: str,
        ban_seconds: Optional[int],
        unban_time: Optional[datetime],
        bounce_count: int,
        permban: bool,
    ) -> None:
        channel = await self._get_log_channel(guild)
        if not channel:
            return
        payload = {
            "member_tag": member_tag,
            "join_time": join_time.timestamp(),
            "leave_time": leave_time.timestamp(),
            "elapsed_seconds": elapsed_seconds,
            "dm_result": dm_result,
            "bounce_count": bounce_count,
            "permban": permban,
            "ban_seconds": ban_seconds,
            "unban_time": unban_time.timestamp() if unban_time else None,
        }
        try:
            view = LogActionLayout(self, guild.id, member_id, payload, show_permban=not permban)
            message = await channel.send(view=view)
            await self._store_log_action(guild.id, member_id, message.id, payload)
        except (Forbidden, HTTPException):
            pass

    async def _store_log_action(
        self, guild_id: int, user_id: int, message_id: int, payload: Dict[str, object]
    ) -> None:
        conf = self.config.guild_from_id(guild_id)
        actions = await conf.log_actions()
        for entry in actions:
            if entry.get("message_id") == message_id:
                return
        actions.append({"user_id": user_id, "message_id": message_id, "payload": payload})
        await conf.log_actions.set(actions[-300:])

    async def _remove_log_action(self, guild_id: int, message_id: int) -> None:
        conf = self.config.guild_from_id(guild_id)
        actions = await conf.log_actions()
        new_actions = [entry for entry in actions if entry.get("message_id") != message_id]
        if len(new_actions) != len(actions):
            await conf.log_actions.set(new_actions)

    async def _restore_log_action_views(self) -> None:
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            conf = self.config.guild(guild)
            actions = await conf.log_actions()
            if not actions:
                continue
            cleaned = []
            for entry in actions:
                user_id = entry.get("user_id")
                message_id = entry.get("message_id")
                payload = entry.get("payload")
                if not user_id or not message_id:
                    continue
                if not isinstance(payload, dict):
                    payload = {
                        "member_tag": "알 수 없음",
                        "join_time": 0.0,
                        "leave_time": 0.0,
                        "elapsed_seconds": 0.0,
                        "dm_result": "알 수 없음",
                        "bounce_count": 0,
                        "permban": False,
                        "ban_seconds": None,
                        "unban_time": None,
                    }
                view = LogActionLayout(self, guild.id, user_id, payload, show_permban=not payload.get("permban"))
                try:
                    self.bot.add_view(view, message_id=message_id)
                    cleaned.append(entry)
                except Exception:
                    continue
            if len(cleaned) != len(actions):
                await conf.log_actions.set(cleaned)

    async def _user_is_admin(self, user: discord.abc.User, guild: discord.Guild) -> bool:
        if user.id == guild.owner_id:
            return True
        if await self.bot.is_owner(user):
            return True
        member = guild.get_member(user.id)
        if not member:
            return False
        return member.guild_permissions.administrator

    async def _handle_log_action(
        self,
        interaction: discord.Interaction,
        action: str,
        guild_id: int,
        user_id: int,
        source_view: Optional[ui.LayoutView] = None,
    ) -> None:
        guild = interaction.guild
        if not guild or guild.id != guild_id:
            await interaction.response.send_message(view=_text_view("서버 정보가 일치하지 않습니다."), ephemeral=True)
            return
        if not await self._user_is_admin(interaction.user, guild):
            await interaction.response.send_message(view=_text_view("관리자만 사용할 수 있습니다."), ephemeral=True)
            return

        if action == "permban":
            try:
                target = guild.get_member(user_id)
                if target:
                    await guild.ban(target, reason="들낙 로그에서 영구 밴", delete_message_seconds=0)
                else:
                    await guild.ban(discord.Object(id=user_id), reason="들낙 로그에서 영구 밴", delete_message_seconds=0)
                await self._remove_tempban(guild, user_id)
                if interaction.message:
                    await self._remove_log_action(guild.id, interaction.message.id)
                    if source_view:
                        status_item = source_view.find_item(ACTION_STATUS_ID)
                        if isinstance(status_item, ui.TextDisplay):
                            action_time = format_dt(_utcnow())
                            new_text = (
                                "**조치**\n"
                                f"영구 밴 (관리자: {interaction.user.mention})\n"
                                f"시간: {action_time}"
                            )
                            if hasattr(status_item, "label"):
                                status_item.label = new_text
                            else:
                                status_item.text = new_text
                        for item in source_view.children:
                            if isinstance(item, ui.ActionRow):
                                for child in item.children:
                                    if isinstance(child, ui.Button):
                                        child.disabled = True
                        try:
                            await interaction.message.edit(view=source_view)
                        except (Forbidden, HTTPException):
                            pass
                await interaction.response.send_message(view=_text_view("영구 밴 완료."), ephemeral=True)
            except (Forbidden, HTTPException) as exc:
                await interaction.response.send_message(view=_text_view(f"영구 밴 실패: {exc}"), ephemeral=True)
            return

        if action == "unban":
            try:
                await guild.unban(discord.Object(id=user_id), reason="들낙 로그에서 밴 해제")
                await self._remove_tempban(guild, user_id)
                if interaction.message:
                    await self._remove_log_action(guild.id, interaction.message.id)
                    if source_view:
                        status_item = source_view.find_item(ACTION_STATUS_ID)
                        if isinstance(status_item, ui.TextDisplay):
                            action_time = format_dt(_utcnow())
                            new_text = (
                                "**조치**\n"
                                f"밴 해제 (관리자: {interaction.user.mention})\n"
                                f"시간: {action_time}"
                            )
                            if hasattr(status_item, "label"):
                                status_item.label = new_text
                            else:
                                status_item.text = new_text
                        for item in source_view.children:
                            if isinstance(item, ui.ActionRow):
                                for child in item.children:
                                    if isinstance(child, ui.Button):
                                        child.disabled = True
                        try:
                            await interaction.message.edit(view=source_view)
                        except (Forbidden, HTTPException):
                            pass
                await interaction.response.send_message(view=_text_view("밴 해제 완료."), ephemeral=True)
            except NotFound:
                await interaction.response.send_message(view=_text_view("현재 밴 상태가 아닙니다."), ephemeral=True)
            except (Forbidden, HTTPException) as exc:
                await interaction.response.send_message(view=_text_view(f"밴 해제 실패: {exc}"), ephemeral=True)
            return

    async def _add_tempban(
        self, guild: discord.Guild, user_id: int, until: datetime, reason: str
    ) -> None:
        async with self.config.guild(guild).tempbans() as tempbans:
            tempbans.append({"user_id": user_id, "expires_at": until.timestamp(), "reason": reason})

    async def _try_mod_tempban(
        self, member: discord.Member, ban_seconds: int, reason: str
    ) -> bool:
        mod = self.bot.get_cog("Mod")
        if not mod:
            return False
        until = _utcnow() + timedelta(seconds=ban_seconds)
        candidates = [
            ("_tempban", (member.guild, member, until, reason)),
            ("tempban_user", (member.guild, member, until, reason)),
            ("tempban_member", (member.guild, member, until, reason)),
            ("tempban", (member.guild, member, until, reason)),
        ]
        for name, args in candidates:
            func = getattr(mod, name, None)
            if not func or not callable(func):
                continue
            try:
                result = func(*args)
                if asyncio.iscoroutine(result):
                    await result
                return True
            except TypeError:
                continue
            except Exception:
                return False
        return False

    async def _remove_tempban(self, guild: discord.Guild, user_id: int) -> None:
        async with self.config.guild(guild).tempbans() as tempbans:
            tempbans[:] = [entry for entry in tempbans if entry["user_id"] != user_id]

    async def _handle_tempban(
        self, member: discord.Member, ban_seconds: int, reason: str
    ) -> Tuple[bool, datetime]:
        unban_time = _utcnow() + timedelta(seconds=ban_seconds)
        if await self._try_mod_tempban(member, ban_seconds, reason):
            return True, unban_time
        try:
            await member.guild.ban(member, reason=reason, delete_message_seconds=0)
        except (Forbidden, HTTPException) as exc:
            return False, unban_time
        await self._add_tempban(member.guild, member.id, unban_time, reason)
        return True, unban_time

    async def _should_trigger_repeat(self, guild: discord.Guild, member: discord.Member) -> bool:
        config = await self.config.guild(guild).repeat_detection()
        if not config.get("enabled"):
            return False
        # Extension point: implement repeat join/leave counter logic here.
        return False

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        if not member.guild:
            return
        if not await self.config.guild(member.guild).welcome_enabled():
            return
        try:
            window_seconds = await self.config.guild(member.guild).window_seconds()
            ban_seconds = await self.config.guild(member.guild).ban_duration_seconds()
            view = ui.LayoutView()
            info_box = ui.Container(accent_color=discord.Color.blurple().value)
            info_box.add_item(ui.TextDisplay("## 🎉 환영합니다!"))
            info_box.add_item(ui.TextDisplay(
                f"**{member.guild.name}**에 오신 것을 환영합니다.\n"
                "서버 이용 전에 간단한 안내 사항을 꼭 확인해 주세요."
            ))
            info_box.add_item(ui.Separator(visible=True))
            info_box.add_item(ui.TextDisplay(
                "**⏰️ 들낙(단시간 입장/퇴장) 안내**\n"
                f"입장 후 **{_format_minutes(window_seconds)}분** 미만으로 퇴장하실 경우,\n"
                f"시스템에 의해 들낙으로 처리되어 **자동 임시 밴 {_format_days(ban_seconds)}일**이 적용됩니다.\n\n"
                "이는 서버 질서 유지를 위한 자동 시스템이며\n"
                "실수나 테스트 입장도 동일하게 적용되니 참고 부탁드립니다."
            ))
            info_box.add_item(ui.TextDisplay(
                "**감사합니다**\n"
                "쾌적하고 안전한 서버 운영을 위해 협조해 주셔서 감사합니다! 🙏\n"
                "즐거운 이용 되세요!"
            ))
            view.add_item(info_box)
            await member.send(view=view)
        except (Forbidden, HTTPException):
            pass
        include_bots = await self.config.guild(member.guild).include_bots()
        if self._should_ignore_member(member, include_bots):
            return
        if not await self.config.guild(member.guild).enabled():
            return
        guild_cache = self.join_cache.setdefault(member.guild.id, {})
        guild_cache[member.id] = _utcnow()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        guild = member.guild
        if not guild:
            return
        include_bots = await self.config.guild(guild).include_bots()
        if self._should_ignore_member(member, include_bots):
            return
        if not await self.config.guild(guild).enabled():
            return
        guild_cache = self.join_cache.setdefault(guild.id, {})
        join_time = guild_cache.pop(member.id, None)
        if not join_time:
            return
        leave_time = _utcnow()
        elapsed = (leave_time - join_time).total_seconds()
        window_seconds = await self.config.guild(guild).window_seconds()
        is_repeat = await self._should_trigger_repeat(guild, member)
        if elapsed > window_seconds and not is_repeat:
            return
        contacts_text, _ = await self._build_contacts(guild)
        async with self.config.guild(guild).bounce_counts() as counts:
            current = counts.get(str(member.id), 0) + 1
            counts[str(member.id)] = current
        is_permban = current >= 3
        ban_seconds = await self.config.guild(guild).ban_duration_seconds()
        planned_unban = _utcnow() + timedelta(seconds=ban_seconds)
        dm_ok, dm_result = await self._send_dm(
            member,
            contacts_text,
            bounce_count=current,
            ban_seconds=None if is_permban else ban_seconds,
            unban_time=None if is_permban else planned_unban,
            permban=is_permban,
        )
        # DM 실패 로그는 출력하지 않음
        await asyncio.sleep(5)
        if is_permban:
            try:
                await guild.ban(
                    member,
                    reason="들낙 감지(자동) - 영구 밴",
                    delete_message_seconds=0,
                )
                await self._remove_tempban(guild, member.id)
                unban_time = None
            except (Forbidden, HTTPException):
                log_channel = await self._get_log_channel(guild)
                if log_channel:
                    try:
                        await log_channel.send(
                            view=_text_view(f"밴 실패: {member} ({member.id})")
                        )
                    except (Forbidden, HTTPException):
                        pass
                return
        else:
            reason = f"들낙 감지(자동) - tempban {_format_duration(ban_seconds)}"
            ban_ok, unban_time = await self._handle_tempban(member, ban_seconds, reason)
            if not ban_ok:
                log_channel = await self._get_log_channel(guild)
                if log_channel:
                    try:
                        await log_channel.send(
                            view=_text_view(f"밴 실패: {member} ({member.id})")
                        )
                    except (Forbidden, HTTPException):
                        pass
                return
        await self._log_action(
            guild=guild,
            member_id=member.id,
            member_tag=str(member),
            join_time=join_time,
            leave_time=leave_time,
            elapsed_seconds=elapsed,
            dm_result="성공" if dm_ok else dm_result,
            ban_seconds=None if is_permban else ban_seconds,
            unban_time=None if is_permban else unban_time,
            bounce_count=current,
            permban=is_permban,
        )

    @tasks.loop(minutes=1)
    async def _unban_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            tempbans = await self.config.guild(guild).tempbans()
            if not tempbans:
                continue
            now = _utcnow().timestamp()
            for entry in list(tempbans):
                if entry["expires_at"] > now:
                    continue
                user_id = entry["user_id"]
                try:
                    await guild.unban(discord.Object(id=user_id), reason="임시 밴 만료")
                except (Forbidden, HTTPException, NotFound):
                    pass
                await self._remove_tempban(guild, user_id)
                await asyncio.sleep(1)

    @_unban_loop.before_loop
    async def _before_unban_loop(self) -> None:
        await self.bot.wait_until_red_ready()

    @tasks.loop(minutes=5)
    async def _cleanup_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        now = _utcnow()
        for guild_id, cache in list(self.join_cache.items()):
            cutoff = now - timedelta(hours=2)
            for user_id, joined_at in list(cache.items()):
                if joined_at < cutoff:
                    cache.pop(user_id, None)
            if not cache:
                self.join_cache.pop(guild_id, None)

    @_cleanup_loop.before_loop
    async def _before_cleanup_loop(self) -> None:
        await self.bot.wait_until_red_ready()

    @commands.group(name="bounce")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def bounce(self, ctx: commands.Context) -> None:
        """들낙 감지 설정."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @bounce.command(name="enable")
    async def bounce_enable(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send(view=_text_view("들낙 감지가 활성화되었습니다."))

    @bounce.command(name="disable")
    async def bounce_disable(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send(view=_text_view("들낙 감지가 비활성화되었습니다."))

    @bounce.command(name="status")
    async def bounce_status(self, ctx: commands.Context) -> None:
        data = await self.config.guild(ctx.guild).all()
        role_ids = data["role_ids"]
        roles_text = ", ".join(f"<@&{role_id}>" for role_id in role_ids) if role_ids else "없음"
        log_channel_id = data["log_channel_id"]
        log_text = f"<#{log_channel_id}>" if log_channel_id else "없음"
        status_lines = [
            f"**상태**: {'켜짐' if data['enabled'] else '꺼짐'}",
            f"**판정 시간**: {data['window_seconds']}초",
            f"**기본 밴 기간**: {_format_duration(data['ban_duration_seconds'])}",
            f"**담당자 역할**: {roles_text}",
            f"**로그 채널**: {log_text}",
            f"**DM 최대 담당자 수**: {data['max_contacts']}",
            f"**봇 포함**: {'예' if data['include_bots'] else '아니오'}",
        ]
        view = ui.LayoutView()
        info_box = ui.Container(accent_color=discord.Color.blurple().value)
        info_box.add_item(ui.TextDisplay("## 📊 Bounce 상태"))
        info_box.add_item(ui.Separator(visible=True))
        for line in status_lines:
            info_box.add_item(ui.TextDisplay(line))
        view.add_item(info_box)
        await ctx.send(view=view)

    @bounce.command(name="window")
    async def bounce_window(self, ctx: commands.Context, seconds: int) -> None:
        if seconds < 10 or seconds > 3600:
            await ctx.send(view=_text_view("판정 시간은 10~3600초 범위로 설정해야 합니다."))
            return
        await self.config.guild(ctx.guild).window_seconds.set(seconds)
        await ctx.send(view=_text_view(f"판정 시간이 {seconds}초로 설정되었습니다."))

    @bounce.command(name="banduration")
    async def bounce_banduration(self, ctx: commands.Context, duration: str) -> None:
        seconds = _parse_duration(duration)
        if seconds is None:
            await ctx.send(view=_text_view("밴 기간 형식이 올바르지 않습니다. 예: 10m, 12h, 1d, 7d"))
            return
        await self.config.guild(ctx.guild).ban_duration_seconds.set(seconds)
        await ctx.send(view=_text_view(f"기본 밴 기간이 {_format_duration(seconds)}로 설정되었습니다."))

    @bounce.group(name="roles", invoke_without_command=True)
    async def bounce_roles(self, ctx: commands.Context, *, roles: Optional[str] = None) -> None:
        if roles is None:
            await ctx.send_help()
            return
        await self._set_roles(ctx, roles)

    @bounce_roles.command(name="list")
    async def bounce_roles_list(self, ctx: commands.Context) -> None:
        role_ids = await self.config.guild(ctx.guild).role_ids()
        if not role_ids:
            await ctx.send(view=_text_view("설정된 담당자 역할이 없습니다."))
            return
        roles_text = ", ".join(f"<@&{role_id}>" for role_id in role_ids)
        await ctx.send(view=_text_view(f"담당자 역할: {roles_text}"))

    @bounce_roles.command(name="clear")
    async def bounce_roles_clear(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).role_ids.set([])
        await ctx.send(view=_text_view("담당자 역할이 모두 제거되었습니다."))

    async def _set_roles(self, ctx: commands.Context, roles: str) -> None:
        role_ids = []
        invalid_tokens = []
        for token in roles.split(","):
            token = token.strip()
            if not token:
                continue
            role_id = None
            if token.startswith("<@&") and token.endswith(">"):
                token = token[3:-1]
            if token.isdigit():
                role_id = int(token)
            if role_id is None:
                invalid_tokens.append(token)
                continue
            role = ctx.guild.get_role(role_id)
            if not role:
                invalid_tokens.append(token)
                continue
            role_ids.append(role_id)
        if invalid_tokens:
            await ctx.send(view=_text_view(f"인식할 수 없는 역할: {', '.join(invalid_tokens)}"))
            return
        await self.config.guild(ctx.guild).role_ids.set(role_ids)
        await ctx.send(view=_text_view("담당자 역할이 설정되었습니다."))

    @bounce.command(name="logchannel")
    async def bounce_logchannel(self, ctx: commands.Context, *, channel: str) -> None:
        if channel.lower() == "off":
            await self.config.guild(ctx.guild).log_channel_id.set(None)
            await ctx.send(view=_text_view("로그 채널이 비활성화되었습니다."))
            return
        try:
            converter = commands.TextChannelConverter()
            text_channel = await converter.convert(ctx, channel)
        except commands.BadArgument:
            await ctx.send(view=_text_view("올바른 채널을 지정하거나 off를 사용해주세요."))
            return
        await self.config.guild(ctx.guild).log_channel_id.set(text_channel.id)
        await ctx.send(view=_text_view(f"로그 채널이 {text_channel.mention}로 설정되었습니다."))

    @bounce.command(name="maxcontacts")
    async def bounce_maxcontacts(self, ctx: commands.Context, count: int) -> None:
        if count < 1 or count > 100:
            await ctx.send(view=_text_view("최대 담당자 수는 1~100 사이여야 합니다."))
            return
        await self.config.guild(ctx.guild).max_contacts.set(count)
        await ctx.send(view=_text_view(f"DM에 포함할 최대 담당자 수가 {count}명으로 설정되었습니다."))

    @bounce.command(name="includebots")
    async def bounce_includebots(self, ctx: commands.Context, value: bool) -> None:
        await self.config.guild(ctx.guild).include_bots.set(value)
        await ctx.send(view=_text_view(f"봇 포함 설정이 {'켜짐' if value else '꺼짐'}으로 변경되었습니다."))

    @bounce.command(name="welcome")
    async def bounce_welcome(self, ctx: commands.Context, value: bool) -> None:
        await self.config.guild(ctx.guild).welcome_enabled.set(value)
        await ctx.send(view=_text_view(f"환영 DM이 {'켜짐' if value else '꺼짐'}으로 설정되었습니다."))

    @bounce.command(name="count")
    async def bounce_count(self, ctx: commands.Context, user: discord.User, value: str) -> None:
        """들낙 누적 횟수를 증감/초기화합니다. 예: !bounce count @user +1, -1, reset"""
        value_lower = value.lower().strip()
        async with self.config.guild(ctx.guild).bounce_counts() as counts:
            current = int(counts.get(str(user.id), 0))
            if value_lower in {"reset", "clear"}:
                new_value = 0
                action_text = "초기화"
            else:
                try:
                    delta = int(value)
                except ValueError:
                    await ctx.send(view=_text_view("형식이 올바르지 않습니다. 예: +1, -1, reset"))
                    return
                new_value = max(0, current + delta)
                if delta > 0:
                    action_text = "증가"
                elif delta < 0:
                    action_text = "감소"
                else:
                    action_text = "변경 없음"
            counts[str(user.id)] = new_value
        await ctx.send(
            view=_text_view(
                "\n".join(
                    [
                        f"대상: {user.mention} ({user.id})",
                        f"조치: {action_text}",
                        f"이전 횟수: {current}",
                        f"현재 횟수: {new_value}",
                    ]
                )
            )
        )


async def setup(bot: Red) -> None:
    await bot.add_cog(Bounce(bot))
