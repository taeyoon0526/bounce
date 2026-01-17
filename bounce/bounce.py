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
            "repeat_detection": {
                "enabled": False,
                "window_minutes": 5,
                "threshold": 3,
            },
            "tempbans": [],
        }
        self.config.register_guild(**default_guild)
        self.join_cache: Dict[int, Dict[int, datetime]] = {}
        self.unban_task = self._unban_loop
        self.cleanup_task = self._cleanup_loop
        self.unban_task.start()
        self.cleanup_task.start()

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
            lines.append(f"{member.id} (<@{member.id}>)")
        remaining = max(0, len(members) - max_contacts)
        if not lines:
            return "담당자 목록이 비어있습니다.", 0
        text = "\n".join(lines)
        if remaining:
            text = f"{text}\n외 {remaining}명"
        return text, len(members)

    async def _send_dm(self, member: discord.Member, contacts_text: str) -> Tuple[bool, str]:
        message = (
            "들낙으로 인해 임시 밴되었다.\n"
            "재고/문의는 아래 담당자에게 DM 하라.\n\n"
            f"{contacts_text}"
        )
        try:
            await member.send(message)
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
        ban_seconds: int,
        unban_time: datetime,
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
        embed.add_field(
            name="밴",
            value=f"기간: {_format_duration(ban_seconds)}\n해제 예정: {format_dt(unban_time)}",
            inline=False,
        )
        try:
            await channel.send(embed=embed)
        except (Forbidden, HTTPException):
            pass

    async def _add_tempban(
        self, guild: discord.Guild, user_id: int, until: datetime, reason: str
    ) -> None:
        async with self.config.guild(guild).tempbans() as tempbans:
            tempbans.append({"user_id": user_id, "expires_at": until.timestamp(), "reason": reason})

    async def _remove_tempban(self, guild: discord.Guild, user_id: int) -> None:
        async with self.config.guild(guild).tempbans() as tempbans:
            tempbans[:] = [entry for entry in tempbans if entry["user_id"] != user_id]

    async def _handle_tempban(
        self, member: discord.Member, ban_seconds: int, reason: str
    ) -> Tuple[bool, datetime]:
        unban_time = _utcnow() + timedelta(seconds=ban_seconds)
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
        ban_seconds = await self.config.guild(guild).ban_duration_seconds()
        reason = f"들낙 감지(자동) - tempban {_format_duration(ban_seconds)}"
        contacts_text, _ = await self._build_contacts(guild)
        dm_ok, dm_result = await self._send_dm(member, contacts_text)
        if not dm_ok:
            log_channel = await self._get_log_channel(guild)
            if log_channel:
                try:
                    await log_channel.send(f"DM 실패: {member} ({member.id}) - {dm_result}")
                except (Forbidden, HTTPException):
                    pass
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
            ban_seconds=ban_seconds,
            unban_time=unban_time,
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


async def setup(bot: Red) -> None:
    await bot.add_cog(Bounce(bot))
