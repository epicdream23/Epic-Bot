import discord
import asyncio
import json
import os
import sys
import datetime
import traceback
import aiohttp
import re
from datetime import timedelta
from discord.ext import commands
from discord import app_commands, Webhook, SelectOption, ui, Embed, Color, Interaction, ButtonStyle, TextStyle, Member, User, VoiceChannel, TextChannel, Role
from telethon import TelegramClient, events

# ===== Configuration Constants =====
DISCORD_TOKEN = "BOT TOKEN"
OWNER_ID = 123456789 # Your Discord User ID
WEBHOOK_AVATAR_PATH = "static/img/Turf_bot.jpg"

# ===== File Paths =====
GUILD_SETTINGS_FILE = "guild_settings.json"
LOCALE_FILE = "locales.json"
TELEGRAM_CONFIG_FILE = "user_configs.json"
TURF_CONFIG_FILE = "turf_config.json"
PRESET_FILE = "turf_presets.json"
PERSISTENT_LIST_DATA_FILE = "persistent_list_data.json"
ACTIVE_BANS_FILE = "active_bans.json"
REMINDER_MESSAGES_FILE = "reminder_messages.json"
PERMISSIONS_FILE = "permissions.json"

# ===== System Constants =====
MAX_MAIN_LIST_SLOTS = 15
ROLE_LIST_IN_NAME = "Teilnehmer"
ROLE_LIST_RESERVE_NAME = "Reserve"
AUTO_LIST_POST_DELAY = 3 # Seconds

# ===== MyBot Class for Better Structure =====
class MyBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.moving_tasks: dict[tuple[int, int], asyncio.Task] = {}
        self.reminder_messages: dict[int, str] = {}
        self.telegram_clients: dict[str, TelegramClient] = {}

# ===== Localization Manager =====
class LocalizationManager:
    def __init__(self, locale_file: str, settings_file: str):
        self.locale_file = locale_file
        self.settings_file = settings_file
        self._locales = self._load_json(self.locale_file)
        self._settings = self._load_json(self.settings_file)
        print(f"[Localization] Loaded {len(self._locales.get('en', {}))} English strings.")
        print(f"[Localization] Loaded {len(self._settings)} guild language settings.")

    def _load_json(self, file_path: str) -> dict:
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[ERROR] Failed to load JSON from {file_path}: {e}")
        return {}

    def _save_settings(self):
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self._settings, f, indent=4)
        except IOError as e:
            print(f"[ERROR] Failed to save settings to {self.settings_file}: {e}")

    def get_language(self, guild_id: int | None) -> str:
        if guild_id is None:
            return "en"
        return self._settings.get(str(guild_id), {}).get("language", "en")

    def set_language(self, guild_id: int, lang: str):
        guild_id_str = str(guild_id)
        if lang not in self._locales:
            raise ValueError(f"Language '{lang}' is not a valid locale.")
        
        if guild_id_str not in self._settings:
            self._settings[guild_id_str] = {}
        self._settings[guild_id_str]["language"] = lang
        self._save_settings()

    def get_string(self, guild_id: int | None, key: str, **kwargs) -> str:
        lang = self.get_language(guild_id)
        string = self._locales.get(lang, {}).get(key)
        
        # --- DIAGNOSTIC CHANGE ---
        # If a key is missing from the file, the bot will now report the exact key name.
        if string is None:
            string = self._locales.get("en", {}).get(key, f"[Translation missing for key: '{key}']")
        
        try:
            return string.format(**kwargs)
        except KeyError as e:
            print(f"[Localization ERROR] Missing format argument {e} for key '{key}' in language '{lang}'")
            # Also improve the formatting error message
            return f"[Formatting error for key: '{key}'. Expected argument: {e}]"

# ===== Bot Setup =====
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.dm_messages = True
bot = MyBot(command_prefix="!", intents=intents)

# ===== Initialize Managers =====
localizer = LocalizationManager(LOCALE_FILE, GUILD_SETTINGS_FILE)

# ===== Load/Save Helper Functions =====
def load_json_file(filepath, is_dict=True):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {} if is_dict else []
    return {} if is_dict else []

def save_json_file(data, filepath):
    try:
        with open(filepath, "w", encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save to {filepath}: {e}")

# ===== Load initial configurations =====
user_configs = load_json_file(TELEGRAM_CONFIG_FILE)
turf_config = load_json_file(TURF_CONFIG_FILE)
turf_presets = load_json_file(PRESET_FILE)
# FIX: Load reminder messages with string keys
bot.reminder_messages = load_json_file(REMINDER_MESSAGES_FILE)

DEFAULT_MESSAGE_PREFIX = "\u2728 Incoming turf report:"
DEFAULT_PRESET = "**Attacker:** {attacker}\n**Begin:** {begin}\n**Zonename:** {zonename}\n**Zonenumber:** {zonenumber}"
DEFAULT_PRESET_NAME = "Default Turf Preset"

def parse_duration(duration_str: str) -> timedelta | None:
    regex = re.compile(r'(\d+)\s*(s|sec|seconds?|m|min|minutes?|h|hr|hours?|d|days?|w|weeks?)\s*', re.I)
    parts = regex.findall(duration_str)
    if not parts: return None
    total_seconds = 0
    for value, unit in parts:
        value_int = int(value)
        unit_lower = unit.lower()
        if unit_lower.startswith('s'): total_seconds += value_int
        elif unit_lower.startswith('m'): total_seconds += value_int * 60
        elif unit_lower.startswith('h'): total_seconds += value_int * 3600
        elif unit_lower.startswith('d'): total_seconds += value_int * 86400
        elif unit_lower.startswith('w'): total_seconds += value_int * 604800
    return timedelta(seconds=total_seconds) if total_seconds > 0 else None

# ===== Ban Management System =====
class BanDMView(ui.View):
    def __init__(self, ban_manager_instance):
        super().__init__(timeout=None)
        self.manager = ban_manager_instance

    @ui.button(label="Update Countdown", style=ButtonStyle.primary, emoji="🔄", custom_id="ban_refresh_button")
    async def refresh_button(self, interaction: Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        user_id = interaction.user.id
        found_guild_id = None
        for g_id, u_id in self.manager.active_bans.keys():
            if u_id == user_id:
                found_guild_id = g_id
                break

        if found_guild_id:
            await self.manager.update_ban_dm(found_guild_id, user_id)
            ban_info = self.manager.active_bans.get((found_guild_id, user_id))
            lang_context_id = found_guild_id
            if ban_info and ban_info.get("dm_message_id"):
                unban_time = datetime.datetime.fromtimestamp(ban_info["unban_timestamp"], tz=datetime.timezone.utc)
                if unban_time > datetime.datetime.now(datetime.timezone.utc):
                    await interaction.followup.send(localizer.get_string(lang_context_id, "ban_dm_refresh_success"), ephemeral=True)
                else:
                    await interaction.followup.send(localizer.get_string(lang_context_id, "ban_dm_refresh_expired"), ephemeral=True)
            else:
                await interaction.followup.send(localizer.get_string(lang_context_id, "ban_dm_refresh_fail_missing"), ephemeral=True)
        else:
            await interaction.followup.send(localizer.get_string(None, "ban_dm_refresh_fail_noban"), ephemeral=True)

class BanManager:
    def __init__(self, bot_instance: MyBot, localizer_instance: LocalizationManager):
        self.bot = bot_instance
        self.localizer = localizer_instance
        self.active_bans = {}
        self.load_bans()

    def load_bans(self):
        raw_data = load_json_file(ACTIVE_BANS_FILE)
        if raw_data:
            self.active_bans = {(int(k.split(',')[0]), int(k.split(',')[1])): v for k, v in raw_data.items()}
            for ban_data_val in self.active_bans.values():
                if "status" not in ban_data_val: ban_data_val["status"] = "active"
        print(f"[BanManager] {len(self.active_bans)} active bans loaded.")

    def save_bans(self):
        save_json_file({f"{k[0]},{k[1]}": v for k, v in self.active_bans.items()}, ACTIVE_BANS_FILE)

    def _generate_ban_embed(self, guild_id: int, reason: str, unban_timestamp: float, status: str = "active") -> Embed:
        guild = self.bot.get_guild(guild_id)
        guild_name = guild.name if guild else "Unknown Server"
        remaining_time = datetime.datetime.fromtimestamp(unban_timestamp, tz=datetime.timezone.utc) - datetime.datetime.now(datetime.timezone.utc)
        if remaining_time.total_seconds() <= 0:
            desc = self.localizer.get_string(guild_id, "ban_expired_dm_desc")
            if status == "unbanned_pending_roles":
                desc += f"\n{self.localizer.get_string(guild_id, 'ban_expired_dm_pending_roles')}"
            title = self.localizer.get_string(guild_id, "ban_expired_dm_title", guild_name=guild_name)
            return Embed(title=title, description=desc, color=Color.green())
        
        title = self.localizer.get_string(guild_id, "ban_active_dm_title", guild_name=guild_name)
        embed = Embed(title=title, description=f"**{self.localizer.get_string(guild_id, 'reason')}:** {reason}", color=Color.red())
        embed.add_field(name=self.localizer.get_string(guild_id, "ban_ends_at"),
                        value=f"<t:{int(unban_timestamp)}:R> (<t:{int(unban_timestamp)}:F>)", inline=False)
        return embed

    async def _restore_roles(self, member: Member, role_ids_to_restore: list[int]):
        if not role_ids_to_restore: return
        guild = member.guild
        roles_to_add = [role for role_id in role_ids_to_restore if (role := guild.get_role(role_id)) and role != guild.default_role]
        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason=self.localizer.get_string(guild.id, "ban_role_restore_reason"))
                print(f"Roles restored for {member.display_name} in {guild.name}")
            except (discord.Forbidden, discord.HTTPException) as e:
                print(f"Could not restore roles for {member.display_name} in {guild.name}: {e}")

    async def update_ban_dm(self, guild_id: int, user_id: int):
        ban_data = self.active_bans.get((guild_id, user_id))
        if not ban_data or not ban_data.get("dm_message_id"): return
        try:
            guild = self.bot.get_guild(guild_id)
            if not guild: return
            user = await self.bot.fetch_user(user_id)
            channel = await user.create_dm()
            message = await channel.fetch_message(ban_data["dm_message_id"])
            embed = self._generate_ban_embed(guild_id, ban_data["reason"], ban_data["unban_timestamp"], ban_data.get("status"))
            await message.edit(embed=embed)
        except (discord.NotFound, discord.Forbidden):
            ban_data["dm_message_id"] = None
            self.save_bans()
        except Exception as e:
            print(f"[BanManager] Error updating ban DM for {user_id}: {e}")

    async def _handle_ban_session(self, guild_id: int, user_id: int):
        ban_key = (guild_id, user_id)
        while ban_key in self.active_bans and self.active_bans[ban_key].get("status") == "active":
            ban_data = self.active_bans[ban_key]
            unban_time = datetime.datetime.fromtimestamp(ban_data["unban_timestamp"], tz=datetime.timezone.utc)
            now = datetime.datetime.now(datetime.timezone.utc)
            if now >= unban_time:
                guild = self.bot.get_guild(guild_id)
                try:
                    user = await self.bot.fetch_user(user_id)
                except discord.NotFound:
                    self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return

                if not guild:
                    self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return
                try:
                    await guild.unban(user, reason=self.localizer.get_string(guild.id, "ban_unban_reason_expired"))
                    invite_link = None
                    try:
                        target_channel = guild.system_channel or next((c for c in guild.text_channels if c.permissions_for(guild.me).create_instant_invite), None)
                        if target_channel:
                            invite = await target_channel.create_invite(max_age=86400, max_uses=1, reason=f"Auto-invite for {user.name} after ban.")
                            invite_link = invite.url
                    except Exception as e:
                        print(f"Could not create invite for unbanned user {user.id}: {e}")
                    
                    if ban_data.get("dm_message_id"):
                        try:
                            dm_channel = await user.create_dm()
                            message = await dm_channel.fetch_message(ban_data["dm_message_id"])
                            status_for_embed = "unbanned_pending_roles" if ban_data.get("roles_to_restore") else "expired"
                            expired_embed = self._generate_ban_embed(guild_id, ban_data["reason"], ban_data["unban_timestamp"], status_for_embed)
                            expired_embed.add_field(name=self.localizer.get_string(guild.id, "ban_rejoin_link"),
                                                    value=invite_link or self.localizer.get_string(guild.id, "ban_invite_failed"))
                            await message.edit(embed=expired_embed, view=None)
                        except (discord.NotFound, discord.Forbidden): pass

                    member = guild.get_member(user_id)
                    if member and ban_data.get("roles_to_restore"):
                        await self._restore_roles(member, ban_data["roles_to_restore"])
                        self.active_bans.pop(ban_key, None)
                    elif ban_data.get("roles_to_restore"):
                        ban_data["status"] = "unbanned_pending_roles"
                    else:
                        self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return
                except discord.NotFound:
                    member = guild.get_member(user_id)
                    if member and ban_data.get("roles_to_restore"):
                        await self._restore_roles(member, ban_data["roles_to_restore"])
                    self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return
                except discord.Forbidden:
                    print(f"No permission to unban user {user_id} in guild {guild.id}")
                    if ban_data.get("roles_to_restore"): ban_data["status"] = "unbanned_pending_roles"; self.save_bans()
                    return
                except Exception as e:
                    print(f"Critical error unbanning user {user_id}: {e}")
                    if ban_data.get("roles_to_restore"): ban_data["status"] = "unbanned_pending_roles"; self.save_bans()
                    return

            seconds_remaining = (unban_time - now).total_seconds()
            sleep_duration = max(1, min(seconds_remaining + 1, 900))
            await asyncio.sleep(sleep_duration)
            
            if ban_key in self.active_bans and self.active_bans[ban_key].get("status") == "active":
                if datetime.datetime.now(datetime.timezone.utc) < unban_time:
                    await self.update_ban_dm(guild_id, user_id)

    async def start_ban(self, interaction: Interaction, member: Member, duration: timedelta, reason: str):
        guild = interaction.guild
        if not guild: return
        gid = guild.id
        if (gid, member.id) in self.active_bans:
            await interaction.response.send_message(self.localizer.get_string(gid, "ban_already_active", user=member.mention), ephemeral=True)
            return
        unban_time = datetime.datetime.now(datetime.timezone.utc) + duration
        unban_timestamp = unban_time.timestamp()
        view = BanDMView(self)
        embed = self._generate_ban_embed(gid, reason, unban_timestamp)
        roles_to_restore = [role.id for role in member.roles if role.id != gid]
        dm_message = None
        try:
            dm_message = await member.send(embed=embed, view=view)
        except discord.Forbidden:
            await interaction.response.send_message(self.localizer.get_string(gid, "ban_dm_failed", user=member.mention), ephemeral=True)
            return
        
        try:
            await guild.ban(member, reason=f"Banned by {interaction.user.display_name}. Reason: {reason}")
        except discord.Forbidden:
            await interaction.response.send_message(self.localizer.get_string(gid, "ban_perm_failed"), ephemeral=True)
            if dm_message: await dm_message.delete()
            return

        self.active_bans[(gid, member.id)] = {"unban_timestamp": unban_timestamp, "reason": reason,
                                              "dm_message_id": dm_message.id if dm_message else None,
                                              "banned_by": interaction.user.id, "roles_to_restore": roles_to_restore, "status": "active"}
        self.save_bans()
        asyncio.create_task(self._handle_ban_session(gid, member.id))
        await interaction.response.send_message(self.localizer.get_string(gid, "ban_success", user=member.mention, duration=str(duration)), ephemeral=False)

    async def manual_unban(self, interaction: Interaction, user: User, reason: str):
        guild = interaction.guild
        if not guild: return
        gid = guild.id
        ban_key = (gid, user.id)
        ban_data = self.active_bans.pop(ban_key, None)
        if ban_data: self.save_bans()
        
        try:
            await guild.unban(user, reason=f"Manually unbanned by {interaction.user.display_name}. Reason: {reason}")
            unban_feedback = self.localizer.get_string(gid, "unban_success", user=user.mention, user_name=user.name, user_disc=user.discriminator)
        except discord.NotFound:
            await interaction.followup.send(self.localizer.get_string(gid, "unban_not_banned", user=user.mention), ephemeral=True)
            return
        except discord.Forbidden:
            await interaction.followup.send(self.localizer.get_string(gid, "unban_perm_failed"), ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(self.localizer.get_string(gid, "unban_error", error=e), ephemeral=True)
            return

        invite_link = None
        target_invite_channel = guild.system_channel or next((c for c in guild.text_channels if c.permissions_for(guild.me).create_instant_invite), None)
        if target_invite_channel:
            try:
                invite = await target_invite_channel.create_invite(max_age=86400, max_uses=1, reason=f"Invite after manual unban for {user.name}")
                invite_link = invite.url
            except Exception as e:
                print(f"Could not create invite after manual unban for {user.id}: {e}")
        
        dm_action_feedback = ""
        original_dm_edited = False
        if ban_data and ban_data.get("dm_message_id"):
            try:
                unban_embed = Embed(title=self.localizer.get_string(gid, "unban_manual_dm_title", guild_name=guild.name),
                                    description=self.localizer.get_string(gid, "unban_manual_dm_desc", admin=interaction.user.display_name, reason=reason),
                                    color=Color.green())
                unban_embed.add_field(name=self.localizer.get_string(gid, "ban_rejoin_link"), value=invite_link or self.localizer.get_string(gid, "ban_invite_failed"))
                dm_channel = await user.create_dm()
                original_dm = await dm_channel.fetch_message(ban_data["dm_message_id"])
                await original_dm.edit(embed=unban_embed, view=None)
                original_dm_edited = True
                dm_action_feedback = f"\n{self.localizer.get_string(gid, 'unban_dm_edited')}"
            except (discord.NotFound, discord.Forbidden): pass
        
        if not original_dm_edited:
            try:
                dm_content = self.localizer.get_string(gid, 'unban_manual_dm_new_content', guild_name=guild.name, reason=reason)
                dm_content += f"\n\n{self.localizer.get_string(gid, 'ban_rejoin_link')}: {invite_link or self.localizer.get_string(gid, 'ban_invite_failed_short')}"
                await user.send(dm_content)
                dm_action_feedback = f"\n{self.localizer.get_string(gid, 'unban_dm_sent')}"
            except discord.Forbidden:
                dm_action_feedback = f"\n{self.localizer.get_string(gid, 'unban_dm_failed_feedback', invite_link=invite_link or 'N/A')}"
        
        await interaction.followup.send(unban_feedback + dm_action_feedback, ephemeral=True)

    async def initialize_sessions_on_ready(self):
        print("[BanManager] Initializing ban sessions...")
        if not self.active_bans: return
        for (guild_id, user_id), ban_data in list(self.active_bans.items()):
            if ban_data.get("status") == "active":
                asyncio.create_task(self._handle_ban_session(guild_id, user_id))
        print("[BanManager] Ban sessions initialized.")

class UnbanSelectView(ui.View):
    def __init__(self, ban_manager: BanManager, bot_instance: MyBot, banned_entries: list[discord.BanEntry], reason: str, original_interaction: Interaction):
        super().__init__(timeout=180)
        self.ban_manager = ban_manager
        self.bot_instance = bot_instance
        self.selected_user_id: int | None = None
        self.reason = reason
        self.original_interaction = original_interaction
        gid = original_interaction.guild_id
        
        options = [SelectOption(label=f"{be.user.name}#{be.user.discriminator}"[:100], value=str(be.user.id),
                                description=(f"{self.ban_manager.localizer.get_string(gid, 'reason')}: {be.reason}"[:90] + "...") if be.reason else localizer.get_string(gid, 'no_reason_provided'))
                   for be in banned_entries]
        
        if not options:
            options.append(SelectOption(label=localizer.get_string(gid, "unban_no_users_found"), value="_disabled", default=True))
        
        self.user_select = ui.Select(placeholder=localizer.get_string(gid, "unban_select_placeholder"), options=options, disabled=not options or options[0].value == "_disabled")
        self.user_select.callback = self.select_callback
        
        self.unban_button = ui.Button(label=localizer.get_string(gid, "unban_button_label"), style=ButtonStyle.danger, row=1)
        self.unban_button.callback = self.unban_button_callback
        
        self.add_item(self.user_select)
        self.add_item(self.unban_button)

    async def select_callback(self, interaction: Interaction):
        self.selected_user_id = int(self.user_select.values[0])
        await interaction.response.defer()

    async def unban_button_callback(self, interaction: Interaction):
        gid = interaction.guild_id
        if not self.selected_user_id:
            await interaction.response.send_message(localizer.get_string(gid, "unban_select_first"), ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            user_to_unban = await self.bot_instance.fetch_user(self.selected_user_id)
        except discord.NotFound:
            await interaction.followup.send(localizer.get_string(gid, "user_not_found"), ephemeral=True)
            return

        await self.ban_manager.manual_unban(interaction, user_to_unban, self.reason)
        
        self.stop()
        try:
            await self.original_interaction.edit_original_response(
                content=localizer.get_string(gid, "unban_action_processed", user=user_to_unban.mention),
                view=None
            )
        except discord.NotFound:
            pass

class PermissionDenied(app_commands.CheckFailure):
    pass

class PermissionsManager:
    def __init__(self, bot_instance: MyBot):
        self.bot = bot_instance
        self.permissions = {}
        self.load_permissions()

    def load_permissions(self):
        self.permissions = load_json_file(PERMISSIONS_FILE)
        print(f"[Permissions] {len(self.permissions)} guild permissions loaded.")

    def save_permissions(self):
        save_json_file(self.permissions, PERMISSIONS_FILE)

    def _get_guild_perms(self, guild_id: int):
        return self.permissions.setdefault(str(guild_id), {"roles": {}, "users": {}})

    def set_permission(self, guild_id: int, command_name: str, target: Role | User | Member, permission: bool | None):
        guild_perms = self._get_guild_perms(guild_id)
        target_id_str = str(target.id)
        target_type = "roles" if isinstance(target, Role) else "users"
        target_perms = guild_perms[target_type].setdefault(target_id_str, {})
        
        if permission is None:
            if command_name in target_perms:
                del target_perms[command_name]
                if not target_perms:
                    del guild_perms[target_type][target_id_str]
        else:
            target_perms[command_name] = "allow" if permission else "deny"
        self.save_permissions()

    def check(self, interaction: Interaction) -> bool:
        if not interaction.guild or not interaction.command: return True
        user = interaction.user
        if user.id == OWNER_ID: return True
        if isinstance(user, Member) and user.guild_permissions.administrator: return True
        
        command_name = interaction.command.name
        guild_id_str = str(interaction.guild.id)
        user_id_str = str(user.id)
        guild_perms = self.permissions.get(guild_id_str)
        if not guild_perms: return False
        
        user_rule = guild_perms.get("users", {}).get(user_id_str, {}).get(command_name)
        if user_rule is not None: return user_rule == "allow"
        
        if isinstance(user, Member):
            sorted_roles = sorted(user.roles, key=lambda r: r.position, reverse=True)
            for role in sorted_roles:
                role_rule = guild_perms.get("roles", {}).get(str(role.id), {}).get(command_name)
                if role_rule is not None: return role_rule == "allow"
        
        return False

def check_permissions():
    async def predicate(interaction: Interaction) -> bool:
        if not permissions_manager.check(interaction):
            # Pass the localized string to the exception
            raise PermissionDenied(localizer.get_string(interaction.guild_id, "permission_denied"))
        return True
    return app_commands.check(predicate)

class PermissionEditorView(ui.View):
    def __init__(self, manager: PermissionsManager, guild: discord.Guild, command_names: list[str], original_interaction: Interaction):
        super().__init__(timeout=300)
        self.manager = manager
        self.guild = guild
        self.command_names = command_names
        self.original_interaction = original_interaction
        self.message: discord.Message | None = None
        gid = guild.id
        
        self.role_select = ui.RoleSelect(placeholder=localizer.get_string(gid, "perms_select_role_placeholder"), max_values=25, row=0)
        self.user_select = ui.UserSelect(placeholder=localizer.get_string(gid, "perms_select_user_placeholder"), max_values=25, row=1)
        
        self.allow_button = ui.Button(label=localizer.get_string(gid, "perms_allow"), style=ButtonStyle.success, row=2)
        self.deny_button = ui.Button(label=localizer.get_string(gid, "perms_deny"), style=ButtonStyle.danger, row=2)
        self.reset_button = ui.Button(label=localizer.get_string(gid, "perms_reset"), style=ButtonStyle.secondary, row=2)
        self.back_button = ui.Button(label=localizer.get_string(gid, "perms_back_button"), style=ButtonStyle.grey, row=3)

        self.allow_button.callback = self.allow_button_callback
        self.deny_button.callback = self.deny_button_callback
        self.reset_button.callback = self.reset_button_callback
        self.back_button.callback = self.back_button_callback
        
        self.add_item(self.role_select)
        self.add_item(self.user_select)
        self.add_item(self.allow_button)
        self.add_item(self.deny_button)
        self.add_item(self.reset_button)
        self.add_item(self.back_button)

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(content=localizer.get_string(self.guild.id, "interaction_timed_out"), view=None, embed=None)
            except discord.NotFound: pass

    def create_permissions_embed(self) -> discord.Embed:
        gid = self.guild.id
        if len(self.command_names) == 1:
            command_name = self.command_names[0]
            embed = Embed(title=localizer.get_string(gid, "perms_edit_embed_title_single", command=command_name),
                          description=localizer.get_string(gid, "perms_edit_embed_desc"), color=Color.orange())
            guild_perms = self.manager._get_guild_perms(gid)
            
            role_perms_allow = [f"<@&{role_id}>" for role_id, perms in guild_perms.get("roles", {}).items() if perms.get(command_name) == "allow"]
            role_perms_deny = [f"<@&{role_id}>" for role_id, perms in guild_perms.get("roles", {}).items() if perms.get(command_name) == "deny"]
            embed.add_field(name=localizer.get_string(gid, "perms_allowed_roles"), value=", ".join(role_perms_allow) or "N/A", inline=False)
            embed.add_field(name=localizer.get_string(gid, "perms_denied_roles"), value=", ".join(role_perms_deny) or "N/A", inline=False)
            
            user_perms_allow = [f"<@{user_id}>" for user_id, perms in guild_perms.get("users", {}).items() if perms.get(command_name) == "allow"]
            user_perms_deny = [f"<@{user_id}>" for user_id, perms in guild_perms.get("users", {}).items() if perms.get(command_name) == "deny"]
            embed.add_field(name=localizer.get_string(gid, "perms_allowed_users"), value=", ".join(user_perms_allow) or "N/A", inline=False)
            embed.add_field(name=localizer.get_string(gid, "perms_denied_users"), value=", ".join(user_perms_deny) or "N/A", inline=False)
            embed.set_footer(text=localizer.get_string(gid, "perms_footer_instant"))
        else:
            embed = Embed(title=localizer.get_string(gid, "perms_edit_embed_title_multi"),
                          description=localizer.get_string(gid, "perms_edit_embed_desc_multi"), color=Color.dark_orange())
            embed.add_field(name=localizer.get_string(gid, "perms_selected_commands"), value="\n".join(f"`/{cmd}`" for cmd in self.command_names), inline=False)
            embed.set_footer(text=localizer.get_string(gid, "perms_footer_overwrite"))
        return embed

    async def _handle_action(self, interaction: Interaction, permission: bool | None, action_name: str):
        gid = interaction.guild_id
        targets = self.role_select.values + self.user_select.values
        if not targets:
            await interaction.response.send_message(localizer.get_string(gid, "perms_no_target_selected"), ephemeral=True)
            return

        for command in self.command_names:
            for target in targets:
                self.manager.set_permission(self.guild.id, command, target, permission)
        
        new_embed = self.create_permissions_embed()
        if self.message:
            await self.message.edit(embed=new_embed)
        
        await interaction.response.send_message(
            localizer.get_string(gid, "perms_action_success", action=action_name, num_targets=len(targets), num_commands=len(self.command_names)),
            ephemeral=True
        )

    async def allow_button_callback(self, interaction: Interaction):
        await self._handle_action(interaction, True, localizer.get_string(interaction.guild_id, "perms_allow"))

    async def deny_button_callback(self, interaction: Interaction):
        await self._handle_action(interaction, False, localizer.get_string(interaction.guild_id, "perms_deny"))

    async def reset_button_callback(self, interaction: Interaction):
        await self._handle_action(interaction, None, localizer.get_string(interaction.guild_id, "perms_reset"))

    async def back_button_callback(self, interaction: Interaction):
        view = AccessDashboardView(self.manager, bot, self.guild, self.original_interaction)
        view.message = self.message
        embed = view.create_initial_embed()
        await interaction.response.edit_message(embed=embed, view=view)

class AccessDashboardView(ui.View):
    def __init__(self, manager: PermissionsManager, bot_instance: MyBot, guild: discord.Guild, original_interaction: Interaction):
        super().__init__(timeout=300)
        self.manager = manager
        self.bot = bot_instance
        self.guild = guild
        self.original_interaction = original_interaction
        self.message: discord.Message | None = None
        self.add_item(self.create_command_select())

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(content=localizer.get_string(self.guild.id, "interaction_timed_out"), view=None, embed=None)
            except discord.NotFound: pass

    def create_command_select(self):
        gid = self.guild.id
        try:
            all_commands = self.bot.tree.get_commands()
            command_options = [SelectOption(label=f"/{cmd.name}", value=cmd.name, description=cmd.description[:100] if cmd.description else "")
                               for cmd in all_commands if cmd.name not in ["restart", "access"]]
            command_options.sort(key=lambda o: o.label)
            max_selectable = min(len(command_options), 25)
            
            select = ui.Select(placeholder=localizer.get_string(gid, "perms_select_command_placeholder"),
                               options=command_options[:25], min_values=1, max_values=max_selectable or 1)
            select.callback = self.on_command_select
            return select
        except Exception as e:
            print(f"[AccessDashboard] Error creating command select: {e}")
            return ui.Select(placeholder="Error loading commands.", disabled=True)

    def create_initial_embed(self) -> Embed:
        return Embed(title=localizer.get_string(self.guild.id, "perms_dashboard_title"),
                     description=localizer.get_string(self.guild.id, "perms_dashboard_desc"),
                     color=Color.blue())

    async def on_command_select(self, interaction: Interaction):
        await interaction.response.defer()
        command_names = interaction.data["values"]
        editor_view = PermissionEditorView(self.manager, self.guild, command_names, self.original_interaction)
        editor_view.message = self.message
        embed = editor_view.create_permissions_embed()
        await interaction.edit_original_response(embed=embed, view=editor_view)

class PersistentListManager:
    def __init__(self, bot_instance: MyBot):
        self.bot = bot_instance
        self.lists_data = {}
        self.load_lists_data()

    async def _ensure_role(self, guild: discord.Guild, role_name: str) -> discord.Role | None:
        if not guild: return None
        role = discord.utils.get(guild.roles, name=role_name)
        if not role:
            try:
                if not guild.me.guild_permissions.manage_roles: return None
                role = await guild.create_role(name=role_name, reason=f"Persistent list system role: {role_name}")
            except (discord.Forbidden, Exception) as e:
                print(f"[ListManager] Error creating role '{role_name}': {e}")
                return None
        return role

    async def _update_member_roles(self, member: Member | None, guild: discord.Guild, list_status: str | None):
        if not member or not guild or not guild.me.guild_permissions.manage_roles: return
        role_in = await self._ensure_role(guild, ROLE_LIST_IN_NAME)
        role_reserve = await self._ensure_role(guild, ROLE_LIST_RESERVE_NAME)
        try:
            if list_status == "main":
                if role_in: await member.add_roles(role_in, reason="Joined main list")
                if role_reserve and role_reserve in member.roles: await member.remove_roles(role_reserve, reason="Moved to main list")
            elif list_status == "reserve":
                if role_reserve: await member.add_roles(role_reserve, reason="Joined reserve list")
                if role_in and role_in in member.roles: await member.remove_roles(role_in, reason="Moved to reserve list")
            elif list_status is None or list_status == "none":
                if role_in and role_in in member.roles: await member.remove_roles(role_in, reason="Left list")
                if role_reserve and role_reserve in member.roles: await member.remove_roles(role_reserve, reason="Left list")
        except (discord.Forbidden, Exception) as e:
            print(f"[ListManager] Error updating roles for {member.display_name}: {e}")

    def load_lists_data(self):
        raw = load_json_file(PERSISTENT_LIST_DATA_FILE)
        if raw:
            self.lists_data = {int(k): v for k, v in raw.items()}

    def save_lists_data(self):
        save_json_file(self.lists_data, PERSISTENT_LIST_DATA_FILE)

    def _get_guild_list_data(self, guild_id: int):
        return self.lists_data.setdefault(guild_id, {"channel_id": None, "message_id": None, "main": [], "reserve": [], "locked": False})

    def generate_list_content_string(self, guild: discord.Guild) -> str:
        gid = guild.id
        guild_data = self._get_guild_list_data(gid)
        header = f"{localizer.get_string(gid, 'list_header')}\n"
        main_list = [f"{i+1}. {guild.get_member(uid).mention if guild.get_member(uid) else f'ID: {uid}'}" for i, uid in enumerate(guild_data["main"])]
        reserve_list = [f"{i+1}. {guild.get_member(uid).mention if guild.get_member(uid) else f'ID: {uid}'}" for i, uid in enumerate(guild_data["reserve"])]
        main_title = localizer.get_string(gid, "list_main_title", count=len(main_list), max=MAX_MAIN_LIST_SLOTS)
        main_content = "\n".join(main_list) or localizer.get_string(gid, "list_empty")
        reserve_title = localizer.get_string(gid, "list_reserve_title", count=len(reserve_list))
        reserve_content = "\n".join(reserve_list) or localizer.get_string(gid, "list_empty")
        footer = f"\n\n{localizer.get_string(gid, 'list_locked_footer' if guild_data.get('locked', False) else 'list_footer')}"
        return f"{header}\n{main_title}\n{main_content}\n\n{reserve_title}\n{reserve_content}{footer}"

    async def update_list_message(self, guild_id: int, interaction: discord.Interaction | None = None, channel: discord.TextChannel | None = None):
        guild_data = self._get_guild_list_data(guild_id)
        if not guild_data["channel_id"] or not guild_data["message_id"]: return
        target_channel = channel or self.bot.get_channel(guild_data["channel_id"])
        if not isinstance(target_channel, TextChannel): return
        try:
            message = await target_channel.fetch_message(guild_data["message_id"])
            view = None if guild_data.get("locked", False) else PersistentListView(self, guild_id)
            await message.edit(content=self.generate_list_content_string(target_channel.guild), view=view)
            self.save_lists_data()
        except discord.NotFound:
            guild_data.update({"message_id": None, "channel_id": None, "main": [], "reserve": []})
            self.save_lists_data()
        except Exception as e:
            print(f"[ListManager] Error updating list message for guild {guild_id}: {e}")

    async def add_user(self, guild_id: int, user_id: int, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, Member): return
        gid = guild_id
        guild_data = self._get_guild_list_data(gid)
        if guild_data.get("locked", False):
            await interaction.response.send_message(localizer.get_string(gid, "list_locked_short"), ephemeral=True); return
        if user_id in guild_data["main"]:
            await interaction.response.send_message(localizer.get_string(gid, "list_err_already_in_main"), ephemeral=True); return
        
        current_list = "main" if len(guild_data["main"]) < MAX_MAIN_LIST_SLOTS else "reserve"
        
        if user_id in guild_data["reserve"]:
            if current_list == "main":
                guild_data["reserve"].remove(user_id)
                guild_data["main"].append(user_id)
                await self._update_member_roles(interaction.user, interaction.guild, "main")
                await interaction.response.send_message(localizer.get_string(gid, "list_msg_promoted_to_main"), ephemeral=True)
            else:
                await interaction.response.send_message(localizer.get_string(gid, "list_err_already_in_reserve"), ephemeral=True)
                return
        elif current_list == "main":
            guild_data["main"].append(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "main")
            await interaction.response.send_message(localizer.get_string(gid, "list_msg_joined_main"), ephemeral=True)
        else:
            guild_data["reserve"].append(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "reserve")
            await interaction.response.send_message(localizer.get_string(gid, "list_msg_joined_reserve"), ephemeral=True)
            
        await self.update_list_message(gid, interaction=interaction)

    async def remove_user(self, guild_id: int, user_id: int, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, Member): return
        gid = guild_id
        guild_data = self._get_guild_list_data(gid)
        if guild_data.get("locked", False):
            await interaction.response.send_message(localizer.get_string(gid, "list_locked_short"), ephemeral=True); return
        
        if user_id in guild_data["main"]:
            guild_data["main"].remove(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "none")
            msg = localizer.get_string(gid, "list_msg_left_main")
            if guild_data["reserve"]:
                promoted_user_id = guild_data["reserve"].pop(0)
                guild_data["main"].append(promoted_user_id)
                if (promoted_member := interaction.guild.get_member(promoted_user_id)):
                    await self._update_member_roles(promoted_member, interaction.guild, "main")
                    if isinstance(interaction.channel, (TextChannel, VoiceChannel, discord.Thread)):
                        try:
                            await interaction.channel.send(localizer.get_string(gid, "list_msg_promoted_notification", user=promoted_member.mention), allowed_mentions=discord.AllowedMentions(users=True))
                        except discord.Forbidden:
                            pass
            await interaction.response.send_message(msg, ephemeral=True)
            await self.update_list_message(gid, interaction=interaction)
        elif user_id in guild_data["reserve"]:
            guild_data["reserve"].remove(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "none")
            await interaction.response.send_message(localizer.get_string(gid, "list_msg_left_reserve"), ephemeral=True)
            await self.update_list_message(gid, interaction=interaction)
        else:
            await interaction.response.send_message(localizer.get_string(gid, "list_err_not_on_list"), ephemeral=True)

    async def move_to_reserve(self, guild_id: int, user_id: int, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, Member): return
        gid = guild_id
        guild_data = self._get_guild_list_data(gid)
        if guild_data.get("locked", False):
            await interaction.response.send_message(localizer.get_string(gid, "list_locked_short"), ephemeral=True); return
        if user_id in guild_data["reserve"]:
            await interaction.response.send_message(localizer.get_string(gid, "list_err_already_in_reserve"), ephemeral=True); return
        
        msg = localizer.get_string(gid, "list_msg_joined_reserve_direct")
        if user_id in guild_data["main"]:
            guild_data["main"].remove(user_id)
            msg = localizer.get_string(gid, "list_msg_moved_to_reserve")

        guild_data["reserve"].append(user_id)
        await self._update_member_roles(interaction.user, interaction.guild, "reserve")
        await interaction.response.send_message(msg, ephemeral=True)
        await self.update_list_message(gid, interaction=interaction)

    async def initialize_lists_on_ready(self):
        print("[ListManager] Initializing lists on ready...")
        for guild_id, data in list(self.lists_data.items()):
            if data.get("channel_id") and data.get("message_id") and not data.get("locked", False):
                if (guild := self.bot.get_guild(guild_id)) and (channel := guild.get_channel(data["channel_id"])):
                    if isinstance(channel, TextChannel):
                        try:
                            await channel.fetch_message(data["message_id"])
                            await self.update_list_message(guild_id, channel=channel)
                        except discord.NotFound:
                            data.update({"channel_id": None, "message_id": None}); self.save_lists_data()
        print("[ListManager] Finished initializing lists.")

    async def start_new_list_programmatic(self, guild: discord.Guild, channel: TextChannel, clear_participants: bool = True):
        guild_id = guild.id
        guild_data = self._get_guild_list_data(guild_id)
        if guild_data.get("message_id") and not guild_data.get("locked", False):
            try:
                if (old_ch := self.bot.get_channel(guild_data["channel_id"])) and isinstance(old_ch, TextChannel):
                    old_msg = await old_ch.fetch_message(guild_data["message_id"])
                    await old_msg.edit(view=None, content=self.generate_list_content_string(guild) + f"\n\n**{localizer.get_string(guild_id, 'list_replaced')}**")
            except Exception as e:
                print(f"[AutoList] Error disabling old list message: {e}")
        
        if clear_participants: guild_data.update({"main": [], "reserve": []})
        guild_data["locked"] = False
        await self._ensure_role(guild, ROLE_LIST_IN_NAME)
        await self._ensure_role(guild, ROLE_LIST_RESERVE_NAME)
        
        try:
            list_message = await channel.send(content=self.generate_list_content_string(guild), view=PersistentListView(self, guild_id))
            guild_data["channel_id"] = channel.id
            guild_data["message_id"] = list_message.id
            self.save_lists_data()
            return True
        except Exception as e:
            print(f"[AutoList] Error creating list programmatically: {e}")
            return False

class PersistentListView(ui.View):
    def __init__(self, manager: PersistentListManager, guild_id: int):
        super().__init__(timeout=None)
        self.manager = manager
        self.guild_id = guild_id
        
        self.join_button = ui.Button(label=localizer.get_string(guild_id, "list_btn_join"), style=ButtonStyle.success, emoji="✅")
        self.leave_button = ui.Button(label=localizer.get_string(guild_id, "list_btn_leave"), style=ButtonStyle.danger, emoji="🗑️")
        self.reserve_button = ui.Button(label=localizer.get_string(guild_id, "list_btn_reserve"), style=ButtonStyle.secondary, emoji="⏳")

        self.join_button.callback = self.join_button_callback
        self.leave_button.callback = self.leave_button_callback
        self.reserve_button.callback = self.reserve_button_callback

        self.add_item(self.join_button)
        self.add_item(self.leave_button)
        self.add_item(self.reserve_button)

    async def join_button_callback(self, interaction: Interaction):
        await self.manager.add_user(self.guild_id, interaction.user.id, interaction)

    async def leave_button_callback(self, interaction: Interaction):
        await self.manager.remove_user(self.guild_id, interaction.user.id, interaction)

    async def reserve_button_callback(self, interaction: Interaction):
        await self.manager.move_to_reserve(self.guild_id, interaction.user.id, interaction)

list_manager = PersistentListManager(bot)
ban_manager = BanManager(bot, localizer)
permissions_manager = PermissionsManager(bot)

def parse_turf_message(msg):
    if "Auf eure Organisation" not in msg: return None
    lines = msg.splitlines()
    attacker = next((l.split("Angriff von ")[1].split(" ver")[0].strip() for l in lines if "Angriff von" in l), "Unknown")
    begin = next((l.split()[-1][:5] for l in lines if l.startswith("Beginn:")), "??:??")
    zonename = next((l.split(":", 1)[1].strip() for l in lines if l.startswith("Zonenname:")), "Unknown")
    zonenumber = next((l.split(":", 1)[1].strip() for l in lines if l.startswith("Zonennummer:")), "Unknown")
    return {"attacker": fix_attacker_casing(attacker), "begin": begin, "zonename": zonename, "zonenumber": zonenumber}

def format_message(user_id, msg):
    config = user_configs.get(str(user_id), {})
    message_format = config.get("message_format", DEFAULT_PRESET)
    telegram_user = config.get("telegram_user", "???")
    parsed = parse_turf_message(msg)
    intro = config.get("custom_intro", DEFAULT_MESSAGE_PREFIX)
    if parsed:
        try:
            return f"{intro}\n" + message_format.format(telegram_user=telegram_user, **parsed).replace("\\n", "\n")
        except KeyError:
            return f"{intro}\n" + DEFAULT_PRESET.format(**parsed)
    return f"{intro}\n{telegram_user}: {msg}"

def fix_attacker_casing(name):
    parts = name.split()
    preserved = {"mc", "e.v.", "ev", "gmbh", "ag"}
    return ' '.join(part if part.lower() in preserved else part.capitalize() for part in parts)

async def start_telegram_client(user_id: str, user: User | Member | None, is_interactive_setup: bool = False):
    config = user_configs.get(user_id)
    if not config:
        if is_interactive_setup and user: await user.send(localizer.get_string(None, "tg_err_no_config"))
        return

    session_name = f"user_{user_id}"
    api_id, api_hash, telegram_user_filter, webhook_url = (config.get(k) for k in ["api_id", "api_hash", "telegram_user", "webhook_url"])
    if not all([api_id, api_hash, telegram_user_filter, webhook_url]):
        if is_interactive_setup and user: await user.send(localizer.get_string(None, "tg_err_incomplete_config"))
        return

    if user_id in bot.telegram_clients and bot.telegram_clients[user_id].is_connected():
        if is_interactive_setup and user:
            try: await user.send(localizer.get_string(None, "tg_already_connected"))
            except discord.Forbidden: pass
        return

    client = TelegramClient(session_name, int(api_id), api_hash)
    
    try: await client.connect()
    except Exception as e:
        if is_interactive_setup and user: await user.send(localizer.get_string(None, "tg_err_connect", error=e))
        return

    if not await client.is_user_authorized():
        if is_interactive_setup and user:
            try:
                await user.send(localizer.get_string(None, "tg_prompt_phone"))
                def check(m): return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)
                phone_msg = await bot.wait_for("message", check=check, timeout=120)
                await client.send_code_request(phone_msg.content.strip())
                await user.send(localizer.get_string(None, "tg_prompt_code"))
                code_msg = await bot.wait_for("message", check=check, timeout=120)
                await client.sign_in(phone_msg.content.strip(), code=code_msg.content.strip())
                await user.send(localizer.get_string(None, "tg_login_success"))
            except Exception as e:
                if client.is_connected(): await client.disconnect()
                try: await user.send(localizer.get_string(None, "tg_login_failed", error=e))
                except discord.Forbidden: pass
                return
        else:
            if client.is_connected(): await client.disconnect()
            return

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        sender = await event.get_sender()
        if sender and hasattr(sender, 'username') and sender.username == telegram_user_filter:
            async with aiohttp.ClientSession() as session:
                try:
                    webhook = Webhook.from_url(webhook_url, session=session)
                    await webhook.send(format_message(user_id, event.raw_text), username=f"Telegram ({telegram_user_filter})")
                    if "Auf eure Organisation" in event.raw_text:
                        await asyncio.sleep(AUTO_LIST_POST_DELAY)
                        target_gid = user_configs.get(user_id, {}).get("guild_id")
                        if target_gid and (target_guild := bot.get_guild(target_gid)) and (turf_chan_id_str := turf_config.get(str(target_gid))):
                           if (target_channel := bot.get_channel(int(turf_chan_id_str))) and isinstance(target_channel, TextChannel):
                                await list_manager.start_new_list_programmatic(target_guild, target_channel)
                except Exception as e:
                    print(f"[Telegram] Failed to forward message for user {user_id}: {e}")

    try:
        await client.start()
        bot.telegram_clients[user_id] = client
        if is_interactive_setup and user:
            await user.send(localizer.get_string(None, "tg_connect_final_success"))
    except Exception as e:
        if client.is_connected(): await client.disconnect()
        if is_interactive_setup and user:
            try: await user.send(localizer.get_string(None, "tg_connect_final_failed", error=e))
            except discord.Forbidden: pass

async def daily_telegram_notice():
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.datetime.now()
        next_run = datetime.datetime.combine(now.date(), datetime.time(0, 0)) + datetime.timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())
        for user_id_str, config in user_configs.items():
            if webhook_url := config.get("webhook_url"):
                try:
                    async with aiohttp.ClientSession() as session:
                        webhook = Webhook.from_url(webhook_url, session=session)
                        await webhook.send("🔁 The bot restarts every 30 minutes. The Telegram webhook is reactivated on every restart.")
                except Exception as e:
                    print(f"[TelegramNotice] Error for user {user_id_str}: {e}")

async def auto_restart_timer():
    await bot.wait_until_ready()
    await asyncio.sleep(1800) # 30 minutes
    print("🔁 [AutoRestart] Timer expired. Initiating restart...")
    # FIX: Use string keys consistently
    save_json_file(bot.reminder_messages, REMINDER_MESSAGES_FILE)
    permissions_manager.save_permissions()
    list_manager.save_lists_data()
    for client in bot.telegram_clients.values():
        if client.is_connected():
            await client.disconnect()
    os.execv(sys.executable, ['python'] + sys.argv)

@bot.tree.command(name="access", description="Manage command permissions for roles and members.")
@app_commands.checks.has_permissions(administrator=True)
async def access(interaction: Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command is only available in a server.", ephemeral=True)
        return
    try:
        view = AccessDashboardView(permissions_manager, bot, interaction.guild, interaction)
        embed = view.create_initial_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()
    except Exception as e:
        print(f"Error in /access command: {e}")
        traceback.print_exc()
        if not interaction.response.is_done():
            await interaction.response.send_message(localizer.get_string(interaction.guild_id, "unknown_error"), ephemeral=True)

@bot.tree.command(name="language", description="Sets the bot's language for this server.")
@app_commands.checks.has_permissions(manage_guild=True)
async def language(interaction: Interaction):
    gid = interaction.guild_id
    if not gid:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    class LanguageSelectView(ui.View):
        def __init__(self):
            super().__init__(timeout=180)
            options = [SelectOption(label="English", value="en", emoji="🇬🇧"),
                       SelectOption(label="Deutsch (German)", value="de", emoji="🇩🇪")]
            self.select = ui.Select(placeholder=localizer.get_string(gid, "lang_select_placeholder"), options=options)
            self.select.callback = self.on_select
            self.add_item(self.select)
            
        async def on_select(self, i: Interaction):
            if not i.guild_id: return
            lang_code = self.select.values[0]
            localizer.set_language(i.guild_id, lang_code)
            await i.response.send_message(localizer.get_string(i.guild_id, "lang_set_success"), ephemeral=True)
            for item in self.children:
                if isinstance(item, (ui.Button, ui.Select)):
                    item.disabled = True
            if i.message:
                try: await i.message.edit(view=self)
                except discord.NotFound: pass

    await interaction.response.send_message(localizer.get_string(gid, "lang_select_prompt"), view=LanguageSelectView(), ephemeral=True)

@bot.tree.command(name="help", description="Shows the help message.")
async def help_command(interaction: Interaction):
    gid = interaction.guild_id
    help_text = localizer.get_string(gid, "help_full_content")
    embed = Embed(
        title=localizer.get_string(gid, "help_title"),
        description=help_text,
        color=Color.blue()
    )
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name="kick", description="Kicks a member and sends them a new invite.")
@app_commands.describe(member="The member to kick", reason="The reason for the kick")
@check_permissions()
async def kick_command(interaction: Interaction, member: Member, reason: str = "No reason provided."):
    if not interaction.guild: return
    gid = interaction.guild.id
    if member.id == interaction.user.id or member.id == bot.user.id:
        await interaction.response.send_message(localizer.get_string(gid, "kick_self_or_bot"), ephemeral=True); return
    if not interaction.guild.me.guild_permissions.kick_members or not interaction.guild.me.guild_permissions.create_instant_invite:
        await interaction.response.send_message(localizer.get_string(gid, "kick_perm_fail"), ephemeral=True); return
    
    await interaction.response.defer(ephemeral=True)
    invite_link = None
    if isinstance(interaction.channel, (TextChannel, VoiceChannel, discord.Thread)):
        try:
            invite = await interaction.channel.create_invite(max_age=3600, max_uses=1, reason=f"Invite for kicked user {member.display_name}")
            invite_link = invite.url
        except Exception as e: print(f"[Kick] Could not create invite: {e}")
    
    dm_sent = False
    if invite_link:
        try:
            embed = Embed(title=localizer.get_string(gid, "kick_dm_title", guild_name=interaction.guild.name),
                          description=f"**{localizer.get_string(gid, 'reason')}:** {reason}", color=Color.orange())
            embed.add_field(name=localizer.get_string(gid, "kick_dm_rejoin_link"), value=invite_link)
            await member.send(embed=embed)
            dm_sent = True
        except discord.Forbidden: pass
        
    try:
        await member.kick(reason=f"Kicked by {interaction.user.display_name}. Reason: {reason}")
        feedback = localizer.get_string(gid, "kick_success", user=member.mention)
        if dm_sent: feedback += f" {localizer.get_string(gid, 'kick_dm_sent')}"
        else: feedback += f" {localizer.get_string(gid, 'kick_dm_failed_feedback', invite_link=invite_link or 'N/A')}"
        
        if isinstance(interaction.channel, (TextChannel, VoiceChannel, discord.Thread)):
            await interaction.channel.send(feedback)
        await interaction.followup.send(localizer.get_string(gid, "kick_action_success"), ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send(localizer.get_string(gid, "kick_higher_role"), ephemeral=True)

@bot.tree.command(name="ban", description="Bans a member for a specified duration with a DM countdown.")
@app_commands.describe(member="The member to ban", duration="Duration of the ban (e.g., 30m, 2h, 5d)", reason="The reason for the ban")
@check_permissions()
async def ban_command(interaction: Interaction, member: Member, duration: str, reason: str = "No reason provided."):
    if not interaction.guild or not interaction.guild.me.guild_permissions.ban_members: return
    gid = interaction.guild.id
    if member.id == interaction.user.id or member.id == bot.user.id:
        await interaction.response.send_message(localizer.get_string(gid, "ban_self_or_bot"), ephemeral=True); return
    
    parsed_duration = parse_duration(duration)
    if not parsed_duration:
        await interaction.response.send_message(localizer.get_string(gid, "invalid_duration"), ephemeral=True); return
    
    await ban_manager.start_ban(interaction, member, parsed_duration, reason)

@bot.tree.command(name="unban", description="Manually unbans a member and sends an invite link.")
@app_commands.describe(reason="The reason for the manual unban")
@check_permissions()
async def unban_command(interaction: Interaction, reason: str = "Manually unbanned."):
    if not interaction.guild or not interaction.guild.me.guild_permissions.ban_members: return
    gid = interaction.guild.id
    await interaction.response.defer(ephemeral=True, thinking=True)
    
    banned_users = [entry async for entry in interaction.guild.bans(limit=25)]
    
    if not banned_users:
        await interaction.followup.send(localizer.get_string(gid, "unban_no_banned_users"), ephemeral=True); return
        
    view = UnbanSelectView(ban_manager, bot, banned_users, reason, interaction)
    await interaction.followup.send(localizer.get_string(gid, "unban_select_user_prompt"), view=view, ephemeral=True)

@bot.tree.command(name="list_start", description="Starts a new interactive participation list in this channel.")
@check_permissions()
async def list_start_command(interaction: Interaction):
    if not interaction.guild or not isinstance(interaction.channel, TextChannel): return
    gid = interaction.guild.id
    guild_data = list_manager._get_guild_list_data(gid)
    if guild_data.get("message_id") and not guild_data.get("locked", False):
        if (old_channel := interaction.guild.get_channel(guild_data["channel_id"])):
            try:
                await old_channel.fetch_message(guild_data["message_id"])
                await interaction.response.send_message(localizer.get_string(gid, "list_err_already_active", channel=old_channel.mention), ephemeral=True)
                return
            except discord.NotFound: pass
            
    await interaction.response.defer(ephemeral=True)
    await list_manager._ensure_role(interaction.guild, ROLE_LIST_IN_NAME)
    await list_manager._ensure_role(interaction.guild, ROLE_LIST_RESERVE_NAME)
    guild_data.update({"main": [], "reserve": [], "locked": False})
    
    view = PersistentListView(list_manager, gid)
    try:
        list_message = await interaction.channel.send(content=list_manager.generate_list_content_string(interaction.guild), view=view)
        guild_data.update({"channel_id": interaction.channel.id, "message_id": list_message.id})
        list_manager.save_lists_data()
        await interaction.followup.send(localizer.get_string(gid, "list_created_success"), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(localizer.get_string(gid, "list_created_fail", error=e), ephemeral=True)

@bot.tree.command(name="list_lock", description="Locks the active list, removing buttons and roles.")
@check_permissions()
async def list_lock_command(interaction: Interaction):
    if not interaction.guild: return
    gid = interaction.guild.id
    guild_data = list_manager._get_guild_list_data(gid)
    if guild_data.get("locked", False):
        await interaction.response.send_message(localizer.get_string(gid, "list_err_already_locked"), ephemeral=True); return
    if not guild_data.get("message_id"):
        await interaction.response.send_message(localizer.get_string(gid, "list_err_no_list_found"), ephemeral=True); return
        
    await interaction.response.defer(ephemeral=True)
    if (channel := interaction.guild.get_channel(guild_data["channel_id"])) and isinstance(channel, TextChannel):
        try:
            message = await channel.fetch_message(guild_data["message_id"])
            guild_data["locked"] = True
            await message.edit(content=list_manager.generate_list_content_string(interaction.guild), view=None)
        except Exception as e: print(f"Error editing list message on lock: {e}")
        
    feedback = []
    for role_name in [ROLE_LIST_IN_NAME, ROLE_LIST_RESERVE_NAME]:
        if (role := discord.utils.get(interaction.guild.roles, name=role_name)):
            try:
                await role.delete(reason="List locked")
                feedback.append(localizer.get_string(gid, "list_role_deleted", role=role_name))
            except Exception as e:
                feedback.append(localizer.get_string(gid, "list_role_delete_fail", role=role_name, error=e))

    guild_data.update({"locked": True, "main": [], "reserve": []})
    list_manager.save_lists_data()
    final_msg = localizer.get_string(gid, "list_locked_success")
    if feedback: final_msg += "\n" + "\n".join(feedback)
    await interaction.followup.send(final_msg, ephemeral=True)

@bot.tree.command(name="list_refresh", description="Manually refreshes the display of the participation list.")
@check_permissions()
async def list_refresh_command(interaction: Interaction):
    if not interaction.guild: return
    gid = interaction.guild.id
    if not list_manager._get_guild_list_data(gid).get("message_id"):
        await interaction.response.send_message(localizer.get_string(gid, "list_err_no_list_found"), ephemeral=True); return
    
    await interaction.response.defer(ephemeral=True)
    await list_manager.update_list_message(gid, interaction=interaction)
    await interaction.followup.send(localizer.get_string(gid, "list_refreshed"), ephemeral=True)

@bot.tree.command(name="move", description="Move a user between two voice channels repeatedly.")
@app_commands.describe(member="User to move", talk1="First VC", talk2="Second VC", delay="Seconds between moves")
@check_permissions()
async def move(interaction: Interaction, member: Member, talk1: VoiceChannel, talk2: VoiceChannel, delay: float):
    if not interaction.guild: return
    gid = interaction.guild.id
    if delay < 0.1:
        await interaction.response.send_message(localizer.get_string(gid, "move_delay_too_short"), ephemeral=True); return
    
    task_key = (gid, member.id)
    if task_key in bot.moving_tasks:
        bot.moving_tasks[task_key].cancel()
        
    async def move_loop(guild_id, member_id, vc1_id, vc2_id, move_delay):
        current_vc_id = vc1_id
        try:
            while True:
                guild = bot.get_guild(guild_id)
                if not guild: break
                target_member = guild.get_member(member_id)
                if not target_member or not target_member.voice: break
                channel_to_move_to = guild.get_channel(current_vc_id)
                if not isinstance(channel_to_move_to, VoiceChannel): break
                try: await target_member.move_to(channel_to_move_to)
                except discord.HTTPException: break
                current_vc_id = vc2_id if current_vc_id == vc1_id else vc1_id
                await asyncio.sleep(move_delay)
        except asyncio.CancelledError: pass
        finally: bot.moving_tasks.pop((guild_id, member_id), None)
        
    task = asyncio.create_task(move_loop(gid, member.id, talk1.id, talk2.id, delay))
    bot.moving_tasks[task_key] = task
    await interaction.response.send_message(localizer.get_string(gid, "move_started", user=member.display_name), ephemeral=False)

@bot.tree.command(name="stopmove", description="Stop moving a user.")
@app_commands.describe(member="The user to stop moving")
@check_permissions()
async def stopmove(interaction: Interaction, member: Member):
    if not interaction.guild: return
    gid = interaction.guild.id
    if task := bot.moving_tasks.pop((gid, member.id), None):
        task.cancel()
        await interaction.response.send_message(localizer.get_string(gid, "move_stopped", user=member.display_name), ephemeral=False)
    else:
        await interaction.response.send_message(localizer.get_string(gid, "move_not_moving", user=member.display_name), ephemeral=True)

@bot.tree.command(name="reminder", description="Send a reminder DM to a user.")
@app_commands.describe(user="User to remind")
@check_permissions()
async def reminder(interaction: Interaction, user: User):
    if not interaction.guild: return
    gid = interaction.guild.id
    message_to_send = bot.reminder_messages.get(str(gid), "This is a default reminder!")
    try:
        await user.send(f"📌 **{localizer.get_string(gid, 'reminder_dm_prefix', guild_name=interaction.guild.name)}**:\n> {message_to_send}")
        await interaction.response.send_message(localizer.get_string(gid, 'reminder_sent', user=user.mention), ephemeral=False)
    except discord.Forbidden:
        await interaction.response.send_message(localizer.get_string(gid, 'reminder_dm_fail', user=user.mention), ephemeral=False)

@bot.tree.command(name="reminder_edit", description="Edit the reminder message for this server.")
@app_commands.describe(message="The new message to send")
@check_permissions()
async def reminder_edit(interaction: Interaction, message: str):
    if not interaction.guild: return
    gid = interaction.guild.id
    bot.reminder_messages[str(gid)] = message
    save_json_file(bot.reminder_messages, REMINDER_MESSAGES_FILE)
    await interaction.response.send_message(localizer.get_string(gid, 'reminder_edit_success'), ephemeral=False)

@bot.tree.command(name="message_role", description="Send a direct message to all users with a specific role.")
@app_commands.describe(role="The role to message", message="The message to send to each member")
@check_permissions()
async def message_role(interaction: Interaction, role: discord.Role, message: str):
    if not interaction.guild: return
    gid = interaction.guild.id
    await interaction.response.defer(ephemeral=True)
    header = f"**{localizer.get_string(gid, 'message_role_dm_prefix', guild_name=interaction.guild.name, admin_name=interaction.user.display_name)}**\n\n"

    members_to_message = [m for m in role.members if not m.bot]
    total_members = len(members_to_message)
    failed_to_dm = []
    sent_count = 0

    async def send_message_batch(members):
        nonlocal sent_count
        for member in members:
            try:
                await member.send(header + message)
                sent_count += 1
                await asyncio.sleep(0.3)  # Small delay to prevent rate-limiting
            except Exception:
                failed_to_dm.append(member.display_name)

    async def update_progress_bar(current, total):
        filled = int(20 * (current / total))
        bar = "█" * filled + "░" * (20 - filled)
        progress_message = localizer.get_string(gid, "message_role_progress", bar=bar, current=current, total=total)
        try:
            await progress_bar_msg.edit(content=progress_message)
        except discord.NotFound:
            pass  # Handle case where the message might be deleted
        await asyncio.sleep(0.5)  # Short delay to allow UI update


    progress_bar_msg = await interaction.followup.send(localizer.get_string(gid, "message_role_starting"), ephemeral=True)

    if total_members > 50:
        for i in range(0, total_members, 50):
            batch = members_to_message[i:i + 50]
            await send_message_batch(batch)
            await update_progress_bar(sent_count, total_members)
            if i + 50 < total_members:
                await asyncio.sleep(5)  # Wait 5 seconds between batches
    else:
        await send_message_batch(members_to_message)
        await update_progress_bar(sent_count, total_members)

    summary = localizer.get_string(gid, "message_role_summary", sent_count=sent_count, total_members=total_members)
    if failed_to_dm:
        summary += f"\n{localizer.get_string(gid, 'message_role_fail_summary', fail_count=len(failed_to_dm))}"

    await progress_bar_msg.edit(content=summary)

class RoleMessageLangStrings:
    def get_strings(self):
        return {
            "message_role_starting": "Sending messages... please wait. 0/0",
            "message_role_progress": "Progress: [{bar}] {current}/{total}",
            "message_role_summary": "Sent messages to {sent_count} out of {total_members} members.",
            "message_role_fail_summary": "Failed to send to {fail_count} members.",
        }

def add_role_message_lang_strings(localizer: LocalizationManager):
    for lang_code in localizer._locales:
        localizer._locales[lang_code].update(RoleMessageLangStrings().get_strings())

add_role_message_lang_strings(localizer)

@check_permissions()
async def telegram_customize_message(interaction: Interaction, message: str):
    user_id = str(interaction.user.id)
    user_configs.setdefault(user_id, {})["custom_intro"] = message
    save_json_file(user_configs, TELEGRAM_CONFIG_FILE)
    await interaction.response.send_message(localizer.get_string(interaction.guild_id, "tg_custom_intro_updated"), ephemeral=True)

@bot.tree.command(name="turf_edit_default_preset_message", description="Set the default intro line (e.g. before telegram war output).")
@app_commands.describe(message="The message shown above the formatted turf info")
@check_permissions()
async def turf_edit_default_preset_message(interaction: discord.Interaction, message: str):
    user_id = str(interaction.user.id)
    user_configs.setdefault(user_id, {})["custom_intro"] = message
    save_json_file(user_configs, TELEGRAM_CONFIG_FILE)
    await interaction.response.send_message(localizer.get_string(interaction.guild_id, "tg_custom_intro_updated"), ephemeral=True)

@bot.tree.command(name="telegram_save_preset", description="Save a message formatting preset.")
@app_commands.describe(preset_name="Name of your preset")
@check_permissions()
async def telegram_save_preset(interaction: Interaction, preset_name: str):
    user_id = str(interaction.user.id)
    msg_format = user_configs.get(user_id, {}).get("message_format", DEFAULT_PRESET)
    turf_presets.setdefault(user_id, {})[preset_name] = msg_format
    save_json_file(turf_presets, PRESET_FILE)
    await interaction.response.send_message(localizer.get_string(interaction.guild_id, "tg_preset_saved", name=preset_name), ephemeral=True)

class PresetSelect(discord.ui.Select):
    def __init__(self, user_id_str: str, gid: int | None):
        self.user_id_str = user_id_str
        options_dict = {DEFAULT_PRESET_NAME: DEFAULT_PRESET, **turf_presets.get(self.user_id_str, {})}
        options = [SelectOption(label=name, value=name) for name in options_dict.keys()] or [SelectOption(label=localizer.get_string(gid, "tg_no_presets_available"), value="_disabled", default=True)]
        super().__init__(placeholder=localizer.get_string(gid, "tg_select_preset_placeholder"), options=options, min_values=1, max_values=1, disabled=(options[0].value == "_disabled"))
        self.presets_map = options_dict
    
    async def callback(self, interaction: Interaction):
        chosen_preset_name = self.values[0]
        if preset_content := self.presets_map.get(chosen_preset_name):
            user_configs.setdefault(self.user_id_str, {})["message_format"] = preset_content
            save_json_file(user_configs, TELEGRAM_CONFIG_FILE)
            await interaction.response.send_message(localizer.get_string(interaction.guild_id, "tg_preset_loaded", name=chosen_preset_name), ephemeral=True)
            if self.view: self.view.stop()
            if interaction.message:
                try: await interaction.message.edit(content=f"Preset '{chosen_preset_name}' loaded.", view=None)
                except discord.NotFound: pass

class PresetView(discord.ui.View):
    def __init__(self, user_id_str: str, gid: int | None):
        super().__init__(timeout=180)
        self.add_item(PresetSelect(user_id_str, gid))

@bot.tree.command(name="telegram_load_preset", description="Load a previously saved preset.")
@check_permissions()
async def telegram_load_preset(interaction: Interaction):
    user_id_str = str(interaction.user.id)
    gid = interaction.guild_id
    if not turf_presets.get(user_id_str):
        await interaction.response.send_message(localizer.get_string(gid, "tg_no_presets"), ephemeral=True)
        return
    view = PresetView(user_id_str, gid)
    await interaction.response.send_message(localizer.get_string(gid, "tg_load_preset_prompt"), view=view, ephemeral=True)

@bot.tree.command(name="telegram_user_files_clear", description="Deletes ALL of your personal Telegram data from the bot.")
@check_permissions()
async def telegram_user_files_clear(interaction: Interaction):
    await interaction.response.defer(ephemeral=True)
    user_id_str = str(interaction.user.id)
    summary = [f"**{localizer.get_string(None, 'tg_clear_title', user=interaction.user.mention)}**"]
    
    if client := bot.telegram_clients.pop(user_id_str, None):
        if client.is_connected(): await client.disconnect()
        summary.append(localizer.get_string(None, 'tg_clear_disconnect_success'))
    else:
        summary.append(localizer.get_string(None, 'tg_clear_disconnect_none'))
        
    if (user_conf := user_configs.get(user_id_str)) and (webhook_url := user_conf.get("webhook_url")):
        try:
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(webhook_url, session=session)
                await webhook.delete()
                summary.append(localizer.get_string(None, 'tg_clear_webhook_deleted'))
        except (discord.NotFound, ValueError):
            summary.append(localizer.get_string(None, 'tg_clear_webhook_gone'))
        except Exception as e:
            summary.append(localizer.get_string(None, 'tg_clear_webhook_fail', error=e))
            
    if user_configs.pop(user_id_str, None):
        save_json_file(user_configs, TELEGRAM_CONFIG_FILE)
        summary.append(localizer.get_string(None, 'tg_clear_config_removed', file=TELEGRAM_CONFIG_FILE))
        
    if turf_presets.pop(user_id_str, None):
        save_json_file(turf_presets, PRESET_FILE)
        summary.append(localizer.get_string(None, 'tg_clear_config_removed', file=PRESET_FILE))
        
    session_file = f"user_{user_id_str}.session"
    if os.path.exists(session_file):
        try:
            os.remove(session_file)
            summary.append(localizer.get_string(None, 'tg_clear_session_removed', file=session_file))
        except OSError as e:
            summary.append(localizer.get_string(None, 'tg_clear_session_fail', error=e))
    else:
        summary.append(localizer.get_string(None, 'tg_clear_session_none'))
        
    summary.append(f"\n{localizer.get_string(None, 'tg_clear_finished')}")
    await interaction.followup.send("\n".join(summary), ephemeral=True)

@bot.tree.command(name="telegram_set_channel", description="Sets the Discord channel for Telegram war warnings.")
@app_commands.describe(channel="The channel for the warnings")
@check_permissions()
async def telegram_set_channel(interaction: Interaction, channel: TextChannel):
    if not interaction.guild or not isinstance(interaction.user, Member): return
    await interaction.response.defer(ephemeral=True)
    
    user = interaction.user
    user_id_str = str(user.id)
    gid = interaction.guild.id
    
    await interaction.followup.send(localizer.get_string(gid, 'tg_setup_start_feedback', channel=channel.mention), ephemeral=True)
    
    if user_id_str in user_configs and user_configs[user_id_str].get("webhook_url"):
        user_configs[user_id_str]["guild_id"] = gid
        user_configs[user_id_str]["channel_id"] = channel.id
        save_json_file(user_configs, TELEGRAM_CONFIG_FILE)
        try:
            await user.send(localizer.get_string(None, 'tg_setup_reconfigured', channel=channel.mention, guild_name=interaction.guild.name))
        except discord.Forbidden: pass
        await start_telegram_client(user_id_str, user, is_interactive_setup=True)
        return
        
    try:
        await user.send(localizer.get_string(None, 'tg_setup_dm_intro', guild_name=interaction.guild.name))
        def check(m): return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)
        
        await user.send(localizer.get_string(None, 'tg_setup_prompt_api_id'))
        api_id_msg = await bot.wait_for("message", check=check, timeout=300)
        
        await user.send(localizer.get_string(None, 'tg_setup_prompt_api_hash'))
        api_hash_msg = await bot.wait_for("message", check=check, timeout=300)
        
        await user.send(localizer.get_string(None, 'tg_setup_prompt_username'))
        telegram_user_msg = await bot.wait_for("message", check=check, timeout=300)
        
        await user.send(localizer.get_string(None, 'tg_setup_dm_saving'))
        webhook = await channel.create_webhook(name=f"Turf Bot ({user.display_name})")
        
        user_configs[user_id_str] = {"api_id": api_id_msg.content.strip(), "api_hash": api_hash_msg.content.strip(),
                                     "telegram_user": telegram_user_msg.content.strip(), "webhook_url": webhook.url,
                                     "guild_id": gid, "channel_id": channel.id}
        save_json_file(user_configs, TELEGRAM_CONFIG_FILE)
        
        await user.send(localizer.get_string(None, 'tg_setup_dm_saved'))
        await start_telegram_client(user_id_str, user, is_interactive_setup=True)
        
    except asyncio.TimeoutError:
        try: await user.send(localizer.get_string(None, 'tg_setup_timeout'))
        except discord.Forbidden: pass
    except discord.Forbidden:
        await interaction.followup.send(localizer.get_string(gid, 'tg_setup_webhook_fail', channel=channel.name), ephemeral=True)
    except Exception as e:
        try: await user.send(localizer.get_string(None, 'tg_setup_critical_error', error=e))
        except discord.Forbidden: pass

@bot.tree.command(name="restart", description="Restarts the bot (Owner only).")
async def restart_command(interaction: Interaction):
    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message(localizer.get_string(interaction.guild_id, "permission_denied"), ephemeral=True); return
    
    await interaction.response.defer(ephemeral=False)
    
    # FIX: Use string keys consistently
    save_json_file(bot.reminder_messages, REMINDER_MESSAGES_FILE)
    permissions_manager.save_permissions()
    list_manager.save_lists_data()
    
    for client in bot.telegram_clients.values():
        if client.is_connected(): await client.disconnect()
        
    restart_info = {"channel_id": interaction.channel_id}
    try:
        msg = await interaction.followup.send("🔁 Bot is restarting...")
        if msg: restart_info["message_id"] = msg.id
    except Exception as e:
        print(f"[Restart] Could not send restart message: {e}")
        
    save_json_file(restart_info, "restart_info.json")
    os.execv(sys.executable, ['python'] + sys.argv)

@bot.tree.error
async def on_tree_error(interaction: Interaction, error: app_commands.AppCommandError):
    gid = interaction.guild_id
    # Use the message from our custom exception
    if isinstance(error, PermissionDenied):
        msg = str(error)
    elif isinstance(error, app_commands.CheckFailure):
        msg = localizer.get_string(gid, "permission_denied_discord")
    else:
        msg = localizer.get_string(gid, "unknown_error")
        print(f"Ignoring exception in command '{interaction.command.name if interaction.command else 'unknown'}':", file=sys.stderr)
        traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
        
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception as e:
        print(f"Failed to send error message to user: {e}", file=sys.stderr)

@bot.event
async def on_ready():
    print(f"Bot is starting up as {bot.user.name}...")
    bot.add_view(BanDMView(ban_manager))
    await list_manager.initialize_lists_on_ready()
    await ban_manager.initialize_sessions_on_ready()
    
    try:
        synced = await bot.tree.sync()
        print(f"✅ Slash Commands synced: {len(synced)} commands")
    except Exception as e:
        print(f"❌ Error syncing slash commands: {e}")
        
    print("[on_ready] Starting Telegram clients...")
    for user_id_str in list(user_configs.keys()):
        try:
            user_obj = await bot.fetch_user(int(user_id_str))
            await start_telegram_client(user_id_str, user_obj, is_interactive_setup=False)
        except Exception as e:
            print(f"Error starting Telegram client for user {user_id_str}: {e}")
            
    if os.path.exists("restart_info.json"):
        info = load_json_file("restart_info.json")
        if (channel_id := info.get("channel_id")) and (channel := bot.get_channel(channel_id)):
            if isinstance(channel, TextChannel) and (msg_id := info.get("message_id")):
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(content="✅ Bot is back online!")
                except discord.NotFound: pass
        if os.path.exists("restart_info.json"):
            os.remove("restart_info.json")
            
    print("[on_ready] Starting background tasks...")
    asyncio.create_task(daily_telegram_notice())
    asyncio.create_task(auto_restart_timer())
    print("[on_ready] All startup processes complete.")
    print("Logged in as Epic Bot")

@bot.event
async def on_guild_join(guild: discord.Guild):
    print(f"Joined new guild: {guild.name} ({guild.id})")
    target_channel = guild.system_channel or next((c for c in guild.text_channels if c.permissions_for(guild.me).send_messages), None)
    if target_channel:
        gid = guild.id
        welcome_embed = Embed(title=localizer.get_string(gid, "welcome_title"),
                              description=localizer.get_string(gid, "welcome_desc", guild_name=guild.name), color=Color.green())
        welcome_embed.add_field(name=localizer.get_string(gid, "welcome_quickstart_title"),
                                value=localizer.get_string(gid, "welcome_quickstart_content"))
        try:
            await target_channel.send(embed=welcome_embed)
        except discord.Forbidden:
            print(f"Could not send welcome message to {guild.name}")

@bot.event
async def on_member_join(member: Member):
    ban_key = (member.guild.id, member.id)
    if (ban_entry := ban_manager.active_bans.get(ban_key)) and ban_entry.get("status") == "unbanned_pending_roles":
        if roles_ids := ban_entry.get("roles_to_restore"):
            await ban_manager._restore_roles(member, roles_ids)
        ban_manager.active_bans.pop(ban_key, None)
        ban_manager.save_bans()
        print(f"Restored roles for rejoining member {member.id} and cleared ban entry.")

if __name__ == "__main__":
    if not os.path.exists(LOCALE_FILE):
        print(f"[FATAL] Locale file '{LOCALE_FILE}' not found. Please create it. Exiting.")
        sys.exit(1)
    try:
        bot.run(DISCORD_TOKEN)
    except discord.LoginFailure:
        print("[FATAL] Login failed: Invalid Discord token.")
    except Exception as e:
        print(f"[FATAL] An error occurred while running the bot: {e}")