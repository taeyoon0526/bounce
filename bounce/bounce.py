from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord.ext import tasks
from discord.errors import Forbidden, HTTPException, NotFound
from discord.utils import format_dt
from redbot.core import Config, commands
from redbot.core.bot import Red


DURATION_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[mhd])$")


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


class LogActionButton(discord.ui.Button):
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
        )


class LogActionView(discord.ui.View):
    def __init__(
        self,
        cog: "Bounce",
        guild_id: int,
        user_id: int,
        disabled: bool = False,
        show_permban: bool = True,
    ) -> None:
        super().__init__(timeout=None)
        if show_permban:
            self.add_item(LogActionButton(cog, "permban", guild_id, user_id, disabled=disabled))
        self.add_item(LogActionButton(cog, "unban", guild_id, user_id, disabled=disabled))


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
            if permban:
                embed = discord.Embed(
                    title="영구 밴 안내",
                    description=f"안녕하세요. {guild_name} 운영팀입니다.",
                    color=discord.Color.red(),
                )
                embed.add_field(
                    name="사유",
                    value="들낙이 누적 3회 이상 확인되어 영구 밴이 적용되었습니다.",
                    inline=False,
                )
                embed.add_field(
                    name="누적 횟수",
                    value=f"{bounce_count}회",
                    inline=False,
                )
                embed.add_field(
                    name="문의/재검토",
                    value=(
                        "이 조치에 대해 문의가 필요하시면 아래 담당자에게 DM으로 연락해 주세요.\n"
                        "담당자 목록(일부):\n"
                        f"{contacts_text}"
                    ),
                    inline=False,
                )
                embed.set_footer(text="문의 시 상황을 간략히 알려주시면 빠르게 확인하겠습니다.")
                await member.send(embed=embed)
                return True, "성공"

            embed = discord.Embed(
                title="임시 밴 안내",
                description=f"안녕하세요. {guild_name} 운영팀입니다.",
                color=discord.Color.orange(),
            )
            embed.add_field(
                name="사유",
                value="단시간 입장/퇴장 기록이 확인되어 자동 임시 밴이 적용되었습니다.",
                inline=False,
            )
            if ban_seconds is not None and unban_time is not None:
                embed.add_field(
                    name="밴 정보",
                    value=(
                        f"기간: {_format_duration(ban_seconds)}\n"
                        f"해제 예정: {format_dt(unban_time)}"
                    ),
                    inline=False,
                )
            embed.add_field(
                name="누적 횟수",
                value=f"{bounce_count}회",
                inline=False,
            )
            embed.add_field(
                name="문의/재검토",
                value=(
                    "문의가 필요하시면 아래 담당자에게 DM으로 연락해 주세요.\n"
                    "담당자 목록(일부):\n"
                    f"{contacts_text}\n\n"
                    "서버 초대 링크: https://discord.gg/nexiott2"
                ),
                inline=False,
            )
            embed.set_footer(text="문의 시 상황을 간략히 알려주시면 빠르게 확인하겠습니다.")
            await member.send(embed=embed)
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
        embed = discord.Embed(
            title="들낙 감지 - 임시밴",
            color=discord.Color.red(),
            timestamp=_utcnow(),
        )
        embed.add_field(
            name="유저",
            value=f"{member_tag}\n{member_id}\n<@{member_id}>",
            inline=False,
        )
        embed.add_field(
            name="시간",
            value=(
                f"join: {format_dt(join_time)}\n"
                f"leave: {format_dt(leave_time)}\n"
                f"경과: {elapsed_seconds:.1f}초"
            ),
            inline=False,
        )
        embed.add_field(name="DM", value=dm_result, inline=False)
        embed.add_field(name="들낙 누적", value=f"{bounce_count}회", inline=False)
        if permban:
            embed.title = "들낙 감지 - 영구밴"
            embed.add_field(name="밴", value="영구 밴", inline=False)
        elif ban_seconds is not None and unban_time is not None:
            embed.add_field(
                name="밴",
                value=f"기간: {_format_duration(ban_seconds)}\n해제 예정: {format_dt(unban_time)}",
                inline=False,
            )
        try:
            view = LogActionView(self, guild.id, member_id, show_permban=not permban)
            message = await channel.send(embed=embed, view=view)
            await self._store_log_action(guild.id, member_id, message.id)
        except (Forbidden, HTTPException):
            pass

    async def _store_log_action(self, guild_id: int, user_id: int, message_id: int) -> None:
        conf = self.config.guild_from_id(guild_id)
        actions = await conf.log_actions()
        for entry in actions:
            if entry.get("message_id") == message_id:
                return
        actions.append({"user_id": user_id, "message_id": message_id})
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
                if not user_id or not message_id:
                    continue
                view = LogActionView(self, guild.id, user_id)
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
    ) -> None:
        guild = interaction.guild
        if not guild or guild.id != guild_id:
            await interaction.response.send_message("서버 정보가 일치하지 않습니다.", ephemeral=True)
            return
        if not await self._user_is_admin(interaction.user, guild):
            await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
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
                    disabled_view = LogActionView(self, guild.id, user_id, disabled=True)
                    try:
                        await interaction.message.edit(view=disabled_view)
                    except (Forbidden, HTTPException):
                        pass
                await interaction.response.send_message("영구 밴 완료.", ephemeral=True)
            except (Forbidden, HTTPException) as exc:
                await interaction.response.send_message(f"영구 밴 실패: {exc}", ephemeral=True)
            return

        if action == "unban":
            try:
                await guild.unban(discord.Object(id=user_id), reason="들낙 로그에서 밴 해제")
                await self._remove_tempban(guild, user_id)
                if interaction.message:
                    await self._remove_log_action(guild.id, interaction.message.id)
                    disabled_view = LogActionView(self, guild.id, user_id, disabled=True)
                    try:
                        await interaction.message.edit(view=disabled_view)
                    except (Forbidden, HTTPException):
                        pass
                await interaction.response.send_message("밴 해제 완료.", ephemeral=True)
            except NotFound:
                await interaction.response.send_message("현재 밴 상태가 아닙니다.", ephemeral=True)
            except (Forbidden, HTTPException) as exc:
                await interaction.response.send_message(f"밴 해제 실패: {exc}", ephemeral=True)
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
            embed = discord.Embed(
                title="환영합니다!",
                description=(
                    f"**{member.guild.name}**에 오신 것을 환영합니다.\n"
                    "서버 이용 전에 간단한 안내 사항을 꼭 확인해 주세요."
                ),
                color=discord.Color.blurple(),
            )
            embed.add_field(
                name="⏰️ 들낙(단시간 입장/퇴장) 안내",
                value=(
                    f"입장 후 **{_format_minutes(window_seconds)}분** 미만으로 퇴장하실 경우,\n"
                    f"시스템에 의해 들낙으로 처리되어 **자동 임시 밴 {_format_days(ban_seconds)}일**이 적용됩니다.\n\n"
                    "이는 서버 질서 유지를 위한 자동 시스템이며\n"
                    "실수나 테스트 입장도 동일하게 적용되니 참고 부탁드립니다."
                ),
                inline=False,
            )
            embed.add_field(
                name="감사합니다",
                value="쾌적하고 안전한 서버 운영을 위해 협조해 주셔서 감사합니다! 🙏\n즐거운 이용 되세요!",
                inline=False,
            )
            await member.send(embed=embed)
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
        if not dm_ok:
            log_channel = await self._get_log_channel(guild)
            if log_channel:
                try:
                    await log_channel.send(f"DM 실패: {member} ({member.id}) - {dm_result}")
                except (Forbidden, HTTPException):
                    pass
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
                        await log_channel.send(f"밴 실패: {member} ({member.id})")
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
                        await log_channel.send(f"밴 실패: {member} ({member.id})")
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
        await ctx.send("들낙 감지가 활성화되었습니다.")

    @bounce.command(name="disable")
    async def bounce_disable(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("들낙 감지가 비활성화되었습니다.")

    @bounce.command(name="status")
    async def bounce_status(self, ctx: commands.Context) -> None:
        data = await self.config.guild(ctx.guild).all()
        role_ids = data["role_ids"]
        roles_text = ", ".join(f"<@&{role_id}>" for role_id in role_ids) if role_ids else "없음"
        log_channel_id = data["log_channel_id"]
        log_text = f"<#{log_channel_id}>" if log_channel_id else "없음"
        status_lines = [
            f"상태: {'켜짐' if data['enabled'] else '꺼짐'}",
            f"판정 시간: {data['window_seconds']}초",
            f"기본 밴 기간: {_format_duration(data['ban_duration_seconds'])}",
            f"담당자 역할: {roles_text}",
            f"로그 채널: {log_text}",
            f"DM 최대 담당자 수: {data['max_contacts']}",
            f"봇 포함: {'예' if data['include_bots'] else '아니오'}",
        ]
        await ctx.send("\n".join(status_lines))

    @bounce.command(name="window")
    async def bounce_window(self, ctx: commands.Context, seconds: int) -> None:
        if seconds < 10 or seconds > 3600:
            await ctx.send("판정 시간은 10~3600초 범위로 설정해야 합니다.")
            return
        await self.config.guild(ctx.guild).window_seconds.set(seconds)
        await ctx.send(f"판정 시간이 {seconds}초로 설정되었습니다.")

    @bounce.command(name="banduration")
    async def bounce_banduration(self, ctx: commands.Context, duration: str) -> None:
        seconds = _parse_duration(duration)
        if seconds is None:
            await ctx.send("밴 기간 형식이 올바르지 않습니다. 예: 10m, 12h, 1d, 7d")
            return
        await self.config.guild(ctx.guild).ban_duration_seconds.set(seconds)
        await ctx.send(f"기본 밴 기간이 {_format_duration(seconds)}로 설정되었습니다.")

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
            await ctx.send("설정된 담당자 역할이 없습니다.")
            return
        roles_text = ", ".join(f"<@&{role_id}>" for role_id in role_ids)
        await ctx.send(f"담당자 역할: {roles_text}")

    @bounce_roles.command(name="clear")
    async def bounce_roles_clear(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).role_ids.set([])
        await ctx.send("담당자 역할이 모두 제거되었습니다.")

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
            await ctx.send(f"인식할 수 없는 역할: {', '.join(invalid_tokens)}")
            return
        await self.config.guild(ctx.guild).role_ids.set(role_ids)
        await ctx.send("담당자 역할이 설정되었습니다.")

    @bounce.command(name="logchannel")
    async def bounce_logchannel(self, ctx: commands.Context, *, channel: str) -> None:
        if channel.lower() == "off":
            await self.config.guild(ctx.guild).log_channel_id.set(None)
            await ctx.send("로그 채널이 비활성화되었습니다.")
            return
        try:
            converter = commands.TextChannelConverter()
            text_channel = await converter.convert(ctx, channel)
        except commands.BadArgument:
            await ctx.send("올바른 채널을 지정하거나 off를 사용해주세요.")
            return
        await self.config.guild(ctx.guild).log_channel_id.set(text_channel.id)
        await ctx.send(f"로그 채널이 {text_channel.mention}로 설정되었습니다.")

    @bounce.command(name="maxcontacts")
    async def bounce_maxcontacts(self, ctx: commands.Context, count: int) -> None:
        if count < 1 or count > 100:
            await ctx.send("최대 담당자 수는 1~100 사이여야 합니다.")
            return
        await self.config.guild(ctx.guild).max_contacts.set(count)
        await ctx.send(f"DM에 포함할 최대 담당자 수가 {count}명으로 설정되었습니다.")

    @bounce.command(name="includebots")
    async def bounce_includebots(self, ctx: commands.Context, value: bool) -> None:
        await self.config.guild(ctx.guild).include_bots.set(value)
        await ctx.send(f"봇 포함 설정이 {'켜짐' if value else '꺼짐'}으로 변경되었습니다.")

    @bounce.command(name="welcome")
    async def bounce_welcome(self, ctx: commands.Context, value: bool) -> None:
        await self.config.guild(ctx.guild).welcome_enabled.set(value)
        await ctx.send(f"환영 DM이 {'켜짐' if value else '꺼짐'}으로 설정되었습니다.")


async def setup(bot: Red) -> None:
    await bot.add_cog(Bounce(bot))
