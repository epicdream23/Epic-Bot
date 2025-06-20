import discord 
import asyncio
import json
import os
import sys
import datetime
import aiohttp
import re
from datetime import timedelta
from discord.ext import commands 
from discord import app_commands, Webhook, SelectOption, ui, Embed, Color, Interaction, ButtonStyle, TextStyle, Member, User, VoiceChannel, TextChannel 
from telethon import TelegramClient, events 

class MyBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.moving_tasks: dict[tuple[int, int], asyncio.Task] = {}
        self.reminder_messages: dict[int, str] = {}
        self.telegram_clients: dict[str, TelegramClient] = {}

# Configuration Constants
DISCORD_TOKEN = ""
OWNER_ID = 827620956075065375 
WEBHOOK_AVATAR_PATH = "static/img/Turf_bot.jpg"

# JSON Files
TELEGRAM_CONFIG_FILE = "user_configs.json"
TURF_CONFIG_FILE = "turf_config.json"
PRESET_FILE = "turf_presets.json"
PERSISTENT_LIST_DATA_FILE = "persistent_list_data.json"
ACTIVE_BANS_FILE = "active_bans.json"
REMINDER_MESSAGES_FILE = "reminder_messages.json"

# List System
MAX_MAIN_LIST_SLOTS = 15
ROLE_LIST_IN_NAME = "Teilnehmer"
ROLE_LIST_RESERVE_NAME = "Reserve"
AUTO_LIST_POST_DELAY = 3 

# Load initial configurations
user_configs = json.load(open(TELEGRAM_CONFIG_FILE)) if os.path.exists(TELEGRAM_CONFIG_FILE) else {}
turf_config = json.load(open(TURF_CONFIG_FILE)) if os.path.exists(TURF_CONFIG_FILE) else {}
turf_presets = json.load(open(PRESET_FILE)) if os.path.exists(PRESET_FILE) else {}

# Bot Setup
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.dm_messages = True
bot = MyBot(command_prefix="!", intents=intents)

# Load reminder messages
if os.path.exists(REMINDER_MESSAGES_FILE):
    with open(REMINDER_MESSAGES_FILE, "r") as f:
        bot.reminder_messages = {int(k): v for k, v in json.load(f).items()}
else:
    bot.reminder_messages = {}


DEFAULT_MESSAGE_PREFIX = "\u2728 Incoming turf report:"
DEFAULT_PRESET = "**Attacker:** {attacker}\n**Begin:** {begin}\n**Zonename:** {zonename}\n**Zonenumber:** {zonenumber}"
DEFAULT_PRESET_NAME = "Default Turf Preset"

# Helper Function to Parse Duration
def parse_duration(duration_str: str) -> timedelta | None:
    """Parses a duration string like '30m', '1h 30m', '2d' into a timedelta."""
    regex = re.compile(r'(\d+)\s*(s|sec|seconds?|m|min|minutes?|h|hr|hours?|d|days?|w|weeks?)\s*', re.I)
    parts = regex.findall(duration_str)
    if not parts:
        return None
    
    total_seconds = 0
    for value, unit in parts:
        value_int = int(value)
        unit_lower = unit.lower()
        if unit_lower.startswith('s'):
            total_seconds += value_int
        elif unit_lower.startswith('m'):
            total_seconds += value_int * 60
        elif unit_lower.startswith('h'):
            total_seconds += value_int * 3600
        elif unit_lower.startswith('d'):
            total_seconds += value_int * 86400
        elif unit_lower.startswith('w'):
            total_seconds += value_int * 604800
            
    return timedelta(seconds=total_seconds) if total_seconds > 0 else None

# ===== Ban Management System =====

class BanDMView(ui.View):
    def __init__(self, ban_manager_instance):
        super().__init__(timeout=None)
        self.manager = ban_manager_instance

    @ui.button(label="Countdown aktualisieren", style=ButtonStyle.primary, emoji="🔄", custom_id="ban_refresh_button")
    async def refresh_button(self, interaction: Interaction, button: ui.Button):
        # Defer ephemerally, so followups will also be ephemeral by default.
        # This is usually desired for button clicks that just confirm an action.
        await interaction.response.defer(ephemeral=True) 
        user_id = interaction.user.id
        
        found_guild_id = None
        for (gid, uid) in list(self.manager.active_bans.keys()):
            if uid == user_id:
                if (gid, uid) in self.manager.active_bans:
                    found_guild_id = gid
                    break
        
        if found_guild_id:
            await self.manager.update_ban_dm(found_guild_id, user_id)
            
            # Check the status *after* the update attempt to provide accurate feedback
            ban_info = self.manager.active_bans.get((found_guild_id, user_id))
            if ban_info and ban_info.get("dm_message_id"):
                # Check if ban is not yet expired
                unban_time = datetime.datetime.fromtimestamp(ban_info["unban_timestamp"], tz=datetime.timezone.utc)
                if unban_time > datetime.datetime.now(datetime.timezone.utc):
                    await interaction.followup.send("Countdown in deiner DM wurde aktualisiert.", ephemeral=True)
                else:
                    # Ban has expired, DM should reflect this after update_ban_dm
                    await interaction.followup.send("Dein Bann ist abgelaufen. Die DM wurde aktualisiert.", ephemeral=True)
            else:
                # Ban was removed or DM ID cleared during/after update_ban_dm (e.g., message deleted by user)
                await interaction.followup.send("Dieser Bann ist inzwischen abgelaufen oder die DM konnte nicht mehr gefunden/aktualisiert werden.", ephemeral=True)
        else:
            # Ban was not found in active_bans (already expired and cleaned up)
            try:
                await interaction.followup.send("Dieser Bann ist abgelaufen oder wurde aufgehoben.", ephemeral=True)
            except discord.NotFound: # Should be rare if defer succeeded
                pass

class BanManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.active_bans = {}
        self.load_bans()

    def load_bans(self):
        if os.path.exists(ACTIVE_BANS_FILE):
            try:
                with open(ACTIVE_BANS_FILE, "r") as f:
                    raw_data = json.load(f)
                    self.active_bans = {(int(k.split(',')[0]), int(k.split(',')[1])): v for k, v in raw_data.items()}
                    # Ensure status field exists for backward compatibility
                    for ban_data_val in self.active_bans.values():
                        if "status" not in ban_data_val:
                            ban_data_val["status"] = "active" # Default for old entries
                print(f"[BanManager] {len(self.active_bans)} aktive Banns geladen.")
            except Exception as e:
                print(f"[ERROR] Aktive Banns konnten nicht geladen werden: {e}")
                self.active_bans = {}
    
    def save_bans(self):
        try:
            with open(ACTIVE_BANS_FILE, "w") as f:
                serializable_data = {f"{k[0]},{k[1]}": v for k, v in self.active_bans.items()}
                json.dump(serializable_data, f, indent=4)
        except Exception as e:
            print(f"[ERROR] Aktive Banns konnten nicht gespeichert werden: {e}")

    def _generate_ban_embed(self, guild: discord.Guild, reason: str, unban_timestamp: float, status: str | None = "active") -> Embed:
        remaining_time = datetime.datetime.fromtimestamp(unban_timestamp, tz=datetime.timezone.utc) - datetime.datetime.now(datetime.timezone.utc)
        
        if remaining_time.total_seconds() <= 0:
            desc = "Du kannst dem Server jetzt wieder beitreten."
            if status == "unbanned_pending_roles":
                desc += "\nDeine ursprünglichen Rollen werden wiederhergestellt, sobald du dem Server wieder beitrittst."
            return Embed(title=f"Dein Bann von {guild.name} ist abgelaufen!", description=desc, color=Color.green())
            
        embed = Embed(title=f"Du wurdest von {guild.name} gebannt", description=f"**Grund:** {reason}", color=Color.red())
        embed.add_field(name="Bann endet", value=f"<t:{int(unban_timestamp)}:R> (um <t:{int(unban_timestamp)}:F>)", inline=False)
        return embed
    
    async def _restore_roles(self, member: Member, role_ids_to_restore: list[int]):
        if not role_ids_to_restore: return
        guild = member.guild
        roles_to_add = []
        for role_id in role_ids_to_restore:
            role = guild.get_role(role_id)
            if role and role != guild.default_role:
                roles_to_add.append(role)
        
        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason="Automatische Rollenwiederherstellung nach Bannablauf.")
                print(f"[BanManager] Rollen für {member.display_name} ({member.id}) in {guild.name} wiederhergestellt: {[r.name for r in roles_to_add]}")
            except discord.Forbidden:
                print(f"[BanManager] Keine Berechtigung, Rollen für {member.display_name} in {guild.name} wiederherzustellen.")
            except discord.HTTPException as e:
                print(f"[BanManager] Fehler beim Wiederherstellen der Rollen für {member.display_name} in {guild.name}: {e}")

    async def update_ban_dm(self, guild_id: int, user_id: int):
        ban_data = self.active_bans.get((guild_id, user_id))
        if not ban_data or not ban_data.get("dm_message_id"): return

        try:
            guild = self.bot.get_guild(guild_id)
            if not guild: return
            user = await self.bot.fetch_user(user_id)
            channel = await user.create_dm()
            message = await channel.fetch_message(ban_data["dm_message_id"])
            embed = self._generate_ban_embed(guild, ban_data["reason"], ban_data["unban_timestamp"], ban_data.get("status"))
            await message.edit(embed=embed)
        except (discord.NotFound, discord.Forbidden):
            ban_data["dm_message_id"] = None
            self.save_bans()
        except Exception as e:
            print(f"[BanManager] Fehler beim Aktualisieren der Ban-DM für {user_id}: {e}")

    async def _handle_ban_session(self, guild_id: int, user_id: int):
        ban_key = (guild_id, user_id)

        if not (ban_key in self.active_bans and self.active_bans[ban_key].get("status") == "active"):
            return

        while ban_key in self.active_bans and self.active_bans[ban_key].get("status") == "active":
            ban_data = self.active_bans[ban_key]
            unban_time = datetime.datetime.fromtimestamp(ban_data["unban_timestamp"], tz=datetime.timezone.utc)
            now = datetime.datetime.now(datetime.timezone.utc)

            if now >= unban_time:
                print(f"[BanManager] Bannzeit für User {user_id} abgelaufen. Starte Entbannung.")
                guild = self.bot.get_guild(guild_id)
                user = await self.bot.fetch_user(user_id)

                if not guild or not user:
                    print(f"[BanManager] Guild {guild_id} oder User {user_id} nicht gefunden. Entferne Bann-Eintrag.")
                    self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return

                try:
                    await guild.unban(user, reason="Bannzeit abgelaufen.")
                    print(f"[BanManager] {user.name} von {guild.name} entbannt.")
                    
                    invite_link = None
                    try:
                        target_channel = guild.system_channel or next((c for c in guild.text_channels if c.permissions_for(guild.me).create_instant_invite), None)
                        if target_channel:
                            invite = await target_channel.create_invite(max_age=86400, max_uses=1, reason=f"Automatische Einladung für {user.name} nach Bannablauf.")
                            invite_link = invite.url
                    except Exception as e:
                        print(f"[BanManager] Einladung für entbannten User {user.id} konnte nicht erstellt werden: {e}")

                    if ban_data.get("dm_message_id"):
                        try:
                            dm_channel = await user.create_dm()
                            message = await dm_channel.fetch_message(ban_data["dm_message_id"])
                            status_for_embed = "unbanned_pending_roles" if "roles_to_restore" in ban_data and ban_data["roles_to_restore"] else "expired"
                            expired_embed = self._generate_ban_embed(guild, ban_data["reason"], ban_data["unban_timestamp"], status_for_embed)
                            expired_embed.add_field(name="Hier wieder beitreten", value=invite_link or "Einladung konnte nicht erstellt werden. Bitte kontaktiere einen Admin.")
                            await message.edit(embed=expired_embed, view=None) # View is removed as ban is over
                        except (discord.NotFound, discord.Forbidden): pass

                    member = guild.get_member(user_id)
                    if member and "roles_to_restore" in ban_data and ban_data["roles_to_restore"]:
                        print(f"[BanManager] User {user_id} ist bereits im Server {guild.name}. Versuche Rollenwiederherstellung.")
                        await self._restore_roles(member, ban_data["roles_to_restore"])
                        self.active_bans.pop(ban_key, None)
                        print(f"[BanManager] Rollenwiederherstellung für {user_id} versucht, Bann-Eintrag entfernt.")
                    elif "roles_to_restore" in ban_data and ban_data["roles_to_restore"]:
                        ban_data["status"] = "unbanned_pending_roles"
                        print(f"[BanManager] User {user_id} nicht im Server {guild.name}. Bann als 'unbanned_pending_roles' markiert.")
                    else:
                        self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return 

                except discord.NotFound:
                    print(f"[BanManager] User {user_id} war bereits in Guild {guild.id} entbannt (oder Bann nicht gefunden).")
                    member = guild.get_member(user_id)
                    if member and "roles_to_restore" in ban_data and ban_data["roles_to_restore"]:
                        print(f"[BanManager] User {user_id} ist im Server {guild.name} trotz 'Unknown Ban'. Versuche Rollenwiederherstellung.")
                        await self._restore_roles(member, ban_data["roles_to_restore"])
                        self.active_bans.pop(ban_key, None)
                    elif "roles_to_restore" in ban_data and ban_data["roles_to_restore"]:
                         ban_data["status"] = "unbanned_pending_roles"
                         print(f"[BanManager] Bann für {user_id} als 'unbanned_pending_roles' markiert, da Bann nicht gefunden, aber Rollen gespeichert sind.")
                    else:
                        self.active_bans.pop(ban_key, None)
                    self.save_bans()
                    return
                except discord.Forbidden:
                    print(f"[BanManager] Keine Berechtigung zum Entbannen von User {user_id} in Guild {guild.id}.")
                    if "roles_to_restore" in ban_data and ban_data["roles_to_restore"]:
                        ban_data["status"] = "unbanned_pending_roles"
                        self.save_bans()
                    return 
                except Exception as e:
                    print(f"[BanManager] Kritischer Fehler beim Entbannen von User {user_id}: {e}")
                    if "roles_to_restore" in ban_data and ban_data["roles_to_restore"]:
                        ban_data["status"] = "unbanned_pending_roles"
                        self.save_bans()
                    return

            seconds_remaining = (unban_time - now).total_seconds()
            max_sleep_interval = 900 
            sleep_duration = max(1, min(seconds_remaining + 1, max_sleep_interval))
            
            print(f"[BanManager] Nächste Prüfung für User {user_id} in {int(sleep_duration)} Sekunden.")
            await asyncio.sleep(sleep_duration)
            
            if not (ban_key in self.active_bans and self.active_bans[ban_key].get("status") == "active"):
                return 
            
            if datetime.datetime.now(datetime.timezone.utc) < unban_time:
                 await self.update_ban_dm(guild_id, user_id)

    async def start_ban(self, interaction: Interaction, member: Member, duration: timedelta, reason: str):
        if not interaction.guild: return
        guild = interaction.guild
        if (guild.id, member.id) in self.active_bans:
            await interaction.response.send_message(f"{member.mention} ist bereits aktiv gebannt.", ephemeral=True)
            return

        unban_time = datetime.datetime.now(datetime.timezone.utc) + duration
        unban_timestamp = unban_time.timestamp()
        view = BanDMView(self)
        embed = self._generate_ban_embed(guild, reason, unban_timestamp, status="active")

        roles_to_restore = [role.id for role in member.roles if role.id != guild.id] # Exclude @everyone

        dm_message = None
        try:
            dm_message = await member.send(embed=embed, view=view)
        except discord.Forbidden:
            await interaction.response.send_message(f"⚠️ {member.mention} konnte keine DM gesendet werden. Der Bann wurde nicht ausgesprochen.", ephemeral=True)
            return
        
        try:
            await guild.ban(member, reason=f"Gebannt von {interaction.user.display_name} für {duration}. Grund: {reason}")
        except discord.Forbidden:
            await interaction.response.send_message("Ich habe keine Berechtigung, dieses Mitglied zu bannen.", ephemeral=True)
            if dm_message: await dm_message.delete()
            return
        
        self.active_bans[(guild.id, member.id)] = {
            "unban_timestamp": unban_timestamp,
            "reason": reason,
            "dm_message_id": dm_message.id if dm_message else None,
            "banned_by": interaction.user.id,
            "roles_to_restore": roles_to_restore,
            "status": "active" # Mark as active ban
        }
        self.save_bans()
        asyncio.create_task(self._handle_ban_session(guild.id, member.id))
        await interaction.response.send_message(f"🚫 {member.mention} wurde für {duration} gebannt. Eine DM mit dem Countdown wurde gesendet.", ephemeral=False)

    async def manual_unban(self, interaction: Interaction, user: User, reason: str):
        if not interaction.guild: return
        guild = interaction.guild

        ban_key = (guild.id, user.id)
        ban_data = self.active_bans.pop(ban_key, None) # Remove from active bans immediately
        if ban_data:
            self.save_bans()
            print(f"[BanManager] Manuelles Entbannen: Bann-Eintrag für User {user.id} in Guild {guild.id} aus aktiven Banns entfernt.")

        try:
            await guild.unban(user, reason=f"Manuell entbannt von {interaction.user.display_name}. Grund: {reason}")
            print(f"[BanManager] User {user.id} ({user.name}) manuell von {guild.name} entbannt.")
            unban_feedback = f"✅ {user.mention} ({user.name}#{user.discriminator}) wurde erfolgreich entbannt."

        except discord.NotFound:
            print(f"[BanManager] User {user.id} war nicht in Guild {guild.id} gebannt (Discord API).")
            await interaction.followup.send(f"ℹ️ {user.mention} war auf diesem Server nicht gebannt.", ephemeral=True)
            return
        except discord.Forbidden:
            print(f"[BanManager] Keine Berechtigung zum manuellen Entbannen von User {user.id} in Guild {guild.id}.")
            await interaction.followup.send("Ich habe keine Berechtigung, dieses Mitglied zu entbannen.", ephemeral=True)
            return
        except Exception as e:
            print(f"[BanManager] Fehler beim manuellen Entbannen von User {user.id}: {e}")
            await interaction.followup.send(f"Ein Fehler ist beim Entbannen aufgetreten: {e}", ephemeral=True)
            return

        invite_link = None
        target_invite_channel = guild.system_channel or \
                                next((c for c in guild.text_channels if c.permissions_for(guild.me).create_instant_invite), None)
        if target_invite_channel:
            try:
                invite = await target_invite_channel.create_invite(max_age=86400, max_uses=1, reason=f"Einladung nach manuellem Entbannen für {user.name}")
                invite_link = invite.url
            except Exception as e:
                print(f"[BanManager] Einladung konnte nach manuellem Entbannen für {user.id} nicht erstellt werden: {e}")
        else:
            print(f"[BanManager] Konnte keinen geeigneten Channel zum Erstellen einer Einladung für {user.id} finden.")

        # --- DM Logic ---
        dm_action_feedback = "" # For admin feedback
        original_dm_edited_successfully = False

        if ban_data and ban_data.get("dm_message_id"):
            try:
                dm_channel = await user.create_dm()
                original_dm_message = await dm_channel.fetch_message(ban_data["dm_message_id"])
                
                unban_embed = Embed(
                    title=f"Du wurdest von {guild.name} entbannt!",
                    description=f"Du wurdest manuell von {interaction.user.display_name} entbannt.\n**Grund:** {reason}",
                    color=Color.green()
                )
                if invite_link:
                    unban_embed.add_field(name="Hier wieder beitreten", value=invite_link)
                else:
                    unban_embed.add_field(name="Hier wieder beitreten", value="Einladung konnte nicht erstellt werden. Bitte kontaktiere einen Admin.")
                
                await original_dm_message.edit(embed=unban_embed, view=None)
                original_dm_edited_successfully = True
                dm_action_feedback = "\nℹ️ Die ursprüngliche Bann-DM wurde aktualisiert."
                print(f"[BanManager] Originale Bann-DM für {user.id} bearbeitet.")
            except (discord.NotFound, discord.Forbidden) as e:
                print(f"[BanManager] Konnte originale Bann-DM für {user.id} nicht bearbeiten: {e}. Sende neue DM.")
            except Exception as e:
                print(f"[BanManager] Unerwarteter Fehler beim Bearbeiten der originalen Bann-DM für {user.id}: {e}. Sende neue DM.")

        if not original_dm_edited_successfully:
            dm_message_content = f"✅ Du wurdest manuell von '{guild.name}' entbannt.\nGrund: {reason}"
            if invite_link:
                dm_message_content += f"\n\nHier wieder beitreten: {invite_link}"
            else:
                dm_message_content += "\n\nEine Einladung konnte nicht automatisch erstellt werden. Bitte kontaktiere einen Admin, um dem Server wieder beizutreten."
            try:
                await user.send(dm_message_content)
                dm_action_feedback = "\nℹ️ Eine neue DM mit einem Einladungslink wurde an den Benutzer gesendet."
            except discord.Forbidden:
                print(f"[BanManager] Konnte keine (neue) DM an {user.id} nach manuellem Entbannen senden.")
                dm_action_feedback = f"\n⚠️ Konnte keine DM an den Benutzer senden (ggf. DMs blockiert). Invite: {invite_link or 'Nicht erstellt'}"
            except Exception as e:
                print(f"[BanManager] Fehler beim Senden einer neuen DM an {user.id}: {e}")
                dm_action_feedback = f"\n⚠️ Fehler beim Senden einer neuen DM: {e}. Invite: {invite_link or 'Nicht erstellt'}"
        
        unban_feedback += dm_action_feedback
        
        await interaction.followup.send(unban_feedback, ephemeral=True)

    async def initialize_sessions_on_ready(self):
        print("[BanManager] Initialisiere Bann-Sessions...")
        if not self.active_bans: return

        for (guild_id, user_id), ban_data in list(self.active_bans.items()):
            if ban_data.get("status") == "active":
                print(f"[BanManager] Setze aktive Bann-Session für User {user_id} in Guild {guild_id} fort.")
                asyncio.create_task(self._handle_ban_session(guild_id, user_id))
            elif ban_data.get("status") == "unbanned_pending_roles":
                print(f"[BanManager] User {user_id} in Guild {guild_id} ist entbannt, wartet auf Rollenwiederherstellung bei Rejoin.")
        print("[BanManager] Bann-Sessions initialisiert.")


class UnbanSelectView(ui.View):
    def __init__(self, ban_manager_instance: BanManager, bot_instance: MyBot, banned_entries: list[discord.BanEntry], reason: str, original_interaction: Interaction):
        super().__init__(timeout=180)
        self.ban_manager = ban_manager_instance
        self.bot_instance = bot_instance
        self.selected_user_id: int | None = None
        self.reason = reason
        self.original_interaction = original_interaction

        options = []
        if banned_entries:
            for ban_entry in banned_entries:
                user = ban_entry.user
                label = f"{user.name}#{user.discriminator}"
                if len(label) > 100:
                    label = label[:97] + "..."
                options.append(SelectOption(label=label, value=str(user.id), description=f"Grund: {ban_entry.reason}"[:100] if ban_entry.reason else "Kein Grund angegeben."))
        
        if not options:
            options.append(SelectOption(label="Keine Benutzer zum Entbannen", value="_disabled", default=True))
            self.unban_button.disabled = True

        self.user_select = ui.Select(placeholder="Wähle einen Benutzer zum Entbannen", options=options, min_values=1, max_values=1, disabled=not options or options[0].value == "_disabled")
        self.user_select.callback = self.select_callback
        self.add_item(self.user_select)

    async def select_callback(self, interaction: Interaction):
        self.selected_user_id = int(self.user_select.values[0])
        await interaction.response.defer()

    @ui.button(label="Ausgewählten Benutzer entbannen", style=ButtonStyle.danger, row=1)
    async def unban_button(self, interaction: Interaction, button: ui.Button):
        if not self.selected_user_id:
            await interaction.response.send_message("Bitte wähle zuerst einen Benutzer aus der Liste aus.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        user_to_unban = await self.bot_instance.fetch_user(self.selected_user_id)
        if not user_to_unban:
            await interaction.followup.send("Benutzer nicht gefunden.", ephemeral=True)
            return
            
        await self.ban_manager.manual_unban(interaction, user_to_unban, self.reason)
        
        self.stop()
        await self.original_interaction.edit_original_response(content=f"Entbannungsaktion für {user_to_unban.mention} verarbeitet. Details in der Bestätigungsnachricht.", view=None)


# ===== Persistent List System =====

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
                if not guild.me.guild_permissions.manage_roles:
                    return None
                role = await guild.create_role(name=role_name, reason=f"Persistent list system role: {role_name}")
            except discord.Forbidden:
                return None
            except Exception as e:
                print(f"[ListManager] Error creating role '{role_name}': {e}")
                return None
        return role

    async def _update_member_roles(self, member: Member | None, guild: discord.Guild, list_status: str | None):
        if not member or not guild or not guild.me.guild_permissions.manage_roles:
            return

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
        except discord.Forbidden:
            print(f"[ListManager] Forbidden to manage roles for {member.display_name}.")
        except Exception as e:
            print(f"[ListManager] Error updating roles for {member.display_name}: {e}")


    def load_lists_data(self):
        if os.path.exists(PERSISTENT_LIST_DATA_FILE):
            try:
                with open(PERSISTENT_LIST_DATA_FILE, "r") as f:
                    self.lists_data = {int(k): v for k, v in json.load(f).items()}
            except Exception as e:
                print(f"[ERROR] Failed to load persistent list data: {e}")
                self.lists_data = {}

    def save_lists_data(self):
        try:
            with open(PERSISTENT_LIST_DATA_FILE, "w") as f:
                json.dump(self.lists_data, f, indent=4)
        except Exception as e:
            print(f"[ERROR] Failed to save persistent list data: {e}")

    def _get_guild_list_data(self, guild_id: int):
        if guild_id not in self.lists_data:
            self.lists_data[guild_id] = {"channel_id": None, "message_id": None, "main": [], "reserve": [], "locked": False}
        return self.lists_data[guild_id]

    def generate_list_content_string(self, guild: discord.Guild) -> str:
        guild_data = self._get_guild_list_data(guild.id)
        header = "📋 **Teilnahme-Liste** 📋\n"
        main_list_mentions = [f"{i+1}. {member.mention if (member := guild.get_member(user_id)) else f'User ID: {user_id}'}" for i, user_id in enumerate(guild_data["main"])]
        reserve_list_mentions = [f"{i+1}. {member.mention if (member := guild.get_member(user_id)) else f'User ID: {user_id}'}" for i, user_id in enumerate(guild_data["reserve"])]
        main_text_title = f"\n✅ **Hauptliste ({len(guild_data['main'])}/{MAX_MAIN_LIST_SLOTS})** ✅\n"
        main_text_content = "\n".join(main_list_mentions) or "Noch niemand eingetragen."
        reserve_text_title = f"\n⏳ **Reserveliste ({len(guild_data['reserve'])})** ⏳\n"
        reserve_text_content = "\n".join(reserve_list_mentions) or "Noch niemand eingetragen."
        footer = "\n\n"
        if not guild_data.get("locked", False):
            footer += "_Nutze die Buttons zum Eintragen, Austragen oder für die Reserve._"
        else:
            footer += "🔒 **Diese Liste ist gesperrt.** 🔒"
        return f"{header}{main_text_title}{main_text_content}{reserve_text_title}{reserve_text_content}{footer}"

    async def update_list_message(self, guild_id: int, interaction: discord.Interaction | None = None, channel: discord.TextChannel | None = None):
        guild_data = self._get_guild_list_data(guild_id)
        if not guild_data["channel_id"] or not guild_data["message_id"]:
            if interaction and not interaction.response.is_done():
                 await interaction.followup.send("Fehler: Keine aktive Listen-Nachricht gefunden.", ephemeral=True)
            return

        target_channel = channel or self.bot.get_channel(guild_data["channel_id"])
        if not isinstance(target_channel, TextChannel):
            print(f"[ListManager] Channel {guild_data['channel_id']} not found or not a TextChannel.")
            return

        try:
            message = await target_channel.fetch_message(guild_data["message_id"])
            guild = target_channel.guild
            content_str = self.generate_list_content_string(guild)
            view = None if guild_data.get("locked", False) else PersistentListView(self, guild_id)
            await message.edit(content=content_str, view=view)
            self.save_lists_data()
        except discord.NotFound:
            print(f"[ListManager] List message {guild_data['message_id']} not found. Clearing data.")
            guild_data.update({"message_id": None, "channel_id": None, "main": [], "reserve": []})
            self.save_lists_data()
            if interaction and not interaction.response.is_done():
                await interaction.followup.send("Fehler: Listen-Nachricht wurde gelöscht.", ephemeral=True)
        except Exception as e:
            print(f"[ListManager] Error updating list message for guild {guild_id}: {e}")

    async def add_user(self, guild_id: int, user_id: int, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, Member): return
        
        guild_data = self._get_guild_list_data(guild_id)
        member = interaction.user 
        
        if guild_data.get("locked", False):
            await interaction.response.send_message("🔒 Diese Liste ist gesperrt.", ephemeral=True)
            return
        if user_id in guild_data["main"]:
            await interaction.response.send_message("✅ Du bist bereits in der Hauptliste.", ephemeral=True)
            return
        
        current_list = "main" if len(guild_data["main"]) < MAX_MAIN_LIST_SLOTS else "reserve"
        
        if user_id in guild_data["reserve"]:
            if current_list == "main":
                guild_data["reserve"].remove(user_id)
                guild_data["main"].append(user_id)
                await self._update_member_roles(member, interaction.guild, "main")
                await interaction.response.send_message("👍 Du wurdest von der Reserve in die Hauptliste verschoben!", ephemeral=True)
            else:
                await interaction.response.send_message("ℹ️ Die Hauptliste ist voll. Du bist bereits auf der Reserveliste.", ephemeral=True)
                return
        elif current_list == "main":
            guild_data["main"].append(user_id)
            await self._update_member_roles(member, interaction.guild, "main")
            await interaction.response.send_message("👍 Du wurdest zur Hauptliste hinzugefügt!", ephemeral=True)
        else:
            guild_data["reserve"].append(user_id)
            await self._update_member_roles(member, interaction.guild, "reserve")
            await interaction.response.send_message("ℹ️ Die Hauptliste ist voll. Du wurdest zur Reserveliste hinzugefügt.", ephemeral=True)
        
        await self.update_list_message(guild_id, interaction=interaction)

    async def remove_user(self, guild_id: int, user_id: int, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, Member): return
        
        guild_data = self._get_guild_list_data(guild_id)
        
        if guild_data.get("locked", False):
            await interaction.response.send_message("🔒 Diese Liste ist gesperrt.", ephemeral=True)
            return
            
        if user_id in guild_data["main"]:
            guild_data["main"].remove(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "none") 
            
            if guild_data["reserve"]:
                promoted_user_id = guild_data["reserve"].pop(0)
                guild_data["main"].append(promoted_user_id)
                promoted_member = interaction.guild.get_member(promoted_user_id)
                await self._update_member_roles(promoted_member, interaction.guild, "main")
                
                if isinstance(interaction.channel, (TextChannel, VoiceChannel, discord.Thread)):
                    try:
                        await interaction.channel.send(f"{promoted_member.mention if promoted_member else f'User ID {promoted_user_id}'} wurde befördert!", allowed_mentions=discord.AllowedMentions(users=True))
                    except Exception as e:
                        print(f"Error notifying promoted user: {e}")

            await interaction.response.send_message("🗑️ Du wurdest von der Hauptliste entfernt.", ephemeral=True)
            await self.update_list_message(guild_id, interaction=interaction)
        elif user_id in guild_data["reserve"]:
            guild_data["reserve"].remove(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "none")
            await interaction.response.send_message("🗑️ Du wurdest von der Reserveliste entfernt.", ephemeral=True)
            await self.update_list_message(guild_id, interaction=interaction)
        else:
            await interaction.response.send_message("❓ Du warst nicht auf der Liste.", ephemeral=True)

    async def move_to_reserve(self, guild_id: int, user_id: int, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, Member): return

        guild_data = self._get_guild_list_data(guild_id)
        
        if guild_data.get("locked", False):
            await interaction.response.send_message("🔒 Diese Liste ist gesperrt.", ephemeral=True)
            return
        if user_id in guild_data["reserve"]:
            await interaction.response.send_message("⏳ Du bist bereits in der Reserveliste.", ephemeral=True)
            return
            
        if user_id in guild_data["main"]:
            guild_data["main"].remove(user_id)
            await self._update_member_roles(interaction.user, interaction.guild, "main_to_reserve")
        
        guild_data["reserve"].append(user_id)
        await self._update_member_roles(interaction.user, interaction.guild, "reserve")
        
        if user_id in guild_data["main"]:
             await interaction.response.send_message("🔄 Du wurdest von der Hauptliste in die Reserveliste verschoben.", ephemeral=True)
        else:
             await interaction.response.send_message("👍 Du wurdest zur Reserveliste hinzugefügt.", ephemeral=True)
             
        await self.update_list_message(guild_id, interaction=interaction)
            
    async def initialize_lists_on_ready(self):
        print("[ListManager] Initializing lists on ready...")
        for guild_id_str, data in list(self.lists_data.items()):
            guild_id = int(guild_id_str) 
            if data.get("channel_id") and data.get("message_id") and not data.get("locked", False):
                guild = self.bot.get_guild(guild_id)
                if not guild: continue
                channel = guild.get_channel(data["channel_id"])
                if isinstance(channel, TextChannel):
                    try:
                        message = await channel.fetch_message(data["message_id"])
                        content_str = self.generate_list_content_string(guild)
                        view = PersistentListView(self, guild_id)
                        await message.edit(content=content_str, view=view)
                    except discord.NotFound:
                        data.update({"channel_id": None, "message_id": None})
                        self.save_lists_data()
                    except Exception as e:
                        print(f"[ListManager] Error re-activating list: {e}")
                else:
                    data.update({"channel_id": None, "message_id": None})
                    self.save_lists_data()
        print("[ListManager] Finished initializing lists.")

    async def start_new_list_programmatic(self, guild: discord.Guild, channel: TextChannel, clear_participants: bool = True):
        guild_id = guild.id
        guild_data = self._get_guild_list_data(guild_id)
        
        if guild_data.get("message_id") and not guild_data.get("locked", False):
            try:
                old_ch_id = guild_data["channel_id"]
                old_ch = self.bot.get_channel(old_ch_id) if old_ch_id else None
                if isinstance(old_ch, TextChannel):
                    old_msg = await old_ch.fetch_message(guild_data["message_id"])
                    await old_msg.edit(view=None, content=self.generate_list_content_string(guild) + "\n\n**Diese Liste wurde durch eine neue ersetzt.**")
            except Exception as e:
                print(f"[AutoList] Error disabling old list message: {e}")

        if clear_participants:
            guild_data.update({"main": [], "reserve": []})
        guild_data["locked"] = False

        await self._ensure_role(guild, ROLE_LIST_IN_NAME)
        await self._ensure_role(guild, ROLE_LIST_RESERVE_NAME)
        content_str = self.generate_list_content_string(guild)
        view = PersistentListView(self, guild_id)
        try:
            list_message = await channel.send(content=content_str, view=view)
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

    @ui.button(label="Beitreten", style=ButtonStyle.success, emoji="✅")
    async def join_button(self, interaction: Interaction, button: ui.Button):
        await self.manager.add_user(self.guild_id, interaction.user.id, interaction)

    @ui.button(label="Verlassen", style=ButtonStyle.danger, emoji="🗑️")
    async def leave_button(self, interaction: Interaction, button: ui.Button):
        await self.manager.remove_user(self.guild_id, interaction.user.id, interaction)

    @ui.button(label="Auf Reserve", style=ButtonStyle.secondary, emoji="⏳")
    async def reserve_button(self, interaction: Interaction, button: ui.Button):
        await self.manager.move_to_reserve(self.guild_id, interaction.user.id, interaction)

list_manager = PersistentListManager(bot)
ban_manager = BanManager(bot)

# ===== Helper Functions =====
def save_telegram_configs(configs):
    with open(TELEGRAM_CONFIG_FILE, "w") as f:
        json.dump(configs, f, indent=2)

def save_turf_config(data):
    with open(TURF_CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)

def save_turf_presets(data):
    with open(PRESET_FILE, "w") as f:
        json.dump(data, f, indent=2)

def save_reminder_messages(data):
    try:
        with open(REMINDER_MESSAGES_FILE, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save reminder messages: {e}")

# ===== Parsing and Formatting =====
def parse_turf_message(msg):
    if not msg.startswith("Auf eure Organisation"): return None
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
    try:
        return message_format.format(telegram_user=telegram_user, msg=msg).replace("\\n", "\n")
    except KeyError as e:
        return f"{intro}\n{telegram_user}: {msg}\n(Format error: missing key {e})"

def fix_attacker_casing(name):
    parts = name.split()
    preserved = {"mc", "e.v.", "ev", "gmbh", "ag"}
    return ' '.join(part if part.lower() in preserved else part.capitalize() for part in parts)

# ===== TELEGRAM LOGIN HANDLER & OTHER BOT FUNCTIONALITY =====

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
                        await webhook.send("🔁 Der Bot startet alle 30 Minuten neu. Der Telegram-Webhook wird bei jedem Neustart reaktiviert.")
                except Exception as e:
                    print(f"[TelegramNotice] Error for user {user_id_str}: {e}")

# ===== MODIFIED FUNCTION =====
async def start_telegram_client(user_id: str, interaction_user: User | Member | None, is_interactive_setup: bool = False):
    config = user_configs.get(user_id)
    if not config: 
        if is_interactive_setup and interaction_user:
             await interaction_user.send("Fehler: Keine Telegram Konfiguration für dich gefunden.")
        return

    session_name = f"user_{user_id}"
    api_id, api_hash, telegram_user_filter, webhook_url = (config.get(k) for k in ["api_id", "api_hash", "telegram_user", "webhook_url"])

    if not all([api_id, api_hash, telegram_user_filter, webhook_url]):
        if is_interactive_setup and interaction_user:
            await interaction_user.send("Fehler: Deine Telegram Konfiguration ist unvollständig.")
        return

    if user_id in bot.telegram_clients and bot.telegram_clients[user_id].is_connected():
        if is_interactive_setup and interaction_user:
            try: await interaction_user.send("ℹ️ Dein Telegram-Client ist bereits verbunden und aktiv.")
            except discord.Forbidden: pass
        return
    
    client = TelegramClient(session_name, api_id, api_hash)
    
    try:
        await client.connect()
    except Exception as e:
        if is_interactive_setup and interaction_user:
            try: await interaction_user.send(f"Telegram Verbindungsfehler: {e}")
            except discord.Forbidden: pass
        return

    if not await client.is_user_authorized():
        if is_interactive_setup and interaction_user:
            try:
                await interaction_user.send("Bitte gib deine **Telegram Telefonnummer** ein (z.B. +491234567890):")
                def check_phone(m): return m.author.id == interaction_user.id and isinstance(m.channel, discord.DMChannel)
                phone_msg = await bot.wait_for("message", check=check_phone, timeout=120)
                await client.send_code_request(phone_msg.content.strip())
                await interaction_user.send("Code gesendet! Bitte antworte mit dem Code:")
                code_msg = await bot.wait_for("message", check=check_phone, timeout=120)
                await client.sign_in(phone_msg.content.strip(), code=code_msg.content.strip())
                await interaction_user.send("✅ Telegram Login erfolgreich!")
            except Exception as e:
                if interaction_user:
                    try: await interaction_user.send(f"❌ Telegram Login fehlgeschlagen: {e}")
                    except discord.Forbidden: pass
                if client.is_connected(): await client.disconnect()
                return
        else:
            if client.is_connected(): await client.disconnect()
            return

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        sender = await event.get_sender()
        if sender and hasattr(sender, 'username') and sender.username == telegram_user_filter:
            msg_text = event.raw_text
            async with aiohttp.ClientSession() as session:
                try:
                    webhook = Webhook.from_url(webhook_url, session=session)
                    await webhook.send(format_message(user_id, msg_text), username=f"Telegram ({telegram_user_filter})")
                    if msg_text.startswith("Auf eure Organisation"):
                        await asyncio.sleep(AUTO_LIST_POST_DELAY)
                        target_guild_id = user_configs.get(user_id, {}).get("guild_id")
                        guild_turf_channel_id_str = turf_config.get(str(target_guild_id))
                        if target_guild_id and guild_turf_channel_id_str:
                            target_guild = bot.get_guild(target_guild_id)
                            target_channel = bot.get_channel(int(guild_turf_channel_id_str))
                            if target_guild and isinstance(target_channel, TextChannel):
                                await list_manager.start_new_list_programmatic(target_guild, target_channel)
                except Exception as e:
                    print(f"[Telegram] Failed to forward message for user {user_id}: {e}")
                    if isinstance(e, discord.NotFound) and client.is_connected():
                        await client.disconnect()
                        bot.telegram_clients.pop(user_id, None)

    if client:
        try:
            await client.start() # type: ignore
            bot.telegram_clients[user_id] = client
            if is_interactive_setup and interaction_user:
                try: await interaction_user.send("✅ Dein Telegram-Client wurde erfolgreich verbunden und ist aktiv!")
                except discord.Forbidden: pass
        except Exception as e: 
            if is_interactive_setup and interaction_user:
                try: await interaction_user.send(f"Fehler beim finalen Starten des Telegram Clients: {e}")
                except discord.Forbidden: pass
            if client.is_connected(): await client.disconnect()


# ===== COMMANDS =====

# ===== Kick & Ban Commands =====

@bot.tree.command(name="kick", description="Kickt ein Mitglied und sendet ihm eine erneute Einladung.")
@app_commands.describe(member="Das zu kickende Mitglied", reason="Der Grund für den Kick")
@app_commands.checks.has_permissions(kick_members=True)
async def kick_command(interaction: Interaction, member: Member, reason: str = "Kein Grund angegeben."):
    if not interaction.guild: return
    if member.id == interaction.user.id or member.id == bot.user.id:
        await interaction.response.send_message("Du kannst dich selbst oder den Bot nicht kicken!", ephemeral=True)
        return
    if not interaction.guild.me.guild_permissions.kick_members or not interaction.guild.me.guild_permissions.create_instant_invite:
        await interaction.response.send_message("Ich benötige die Berechtigungen 'Mitglieder kicken' und 'Einladung erstellen'.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    invite_link = None
    if isinstance(interaction.channel, (TextChannel, VoiceChannel)):
        try:
            invite = await interaction.channel.create_invite(max_age=3600, max_uses=1, reason=f"Einladung für {member.display_name}")
            invite_link = invite.url
        except Exception as e:
            print(f"[Kick] Einladung konnte nicht erstellt werden: {e}")

    dm_sent = False
    if invite_link:
        try:
            embed = Embed(title=f"Du wurdest von {interaction.guild.name} gekickt", description=f"**Grund:** {reason}", color=Color.orange())
            embed.add_field(name="Link zum Wiederbeitreten", value=f"Falls du zurückkehren möchtest: {invite_link}")
            await member.send(embed=embed)
            dm_sent = True
        except discord.Forbidden:
            print(f"[Kick] Konnte keine DM an {member.display_name} senden.")
    
    try:
        await member.kick(reason=f"Gekickt von {interaction.user.display_name}. Grund: {reason}")
        feedback = f"👢 {member.mention} wurde gekickt."
        if dm_sent:
            feedback += " Eine Einladung zum Wiederbeitreten wurde per DM gesendet."
        else:
            feedback += f" ⚠️ Es konnte keine DM gesendet werden. Hier ist der Link: {invite_link or 'Konnte nicht erstellt werden.'}"
        
        if isinstance(interaction.channel, (TextChannel, VoiceChannel, discord.Thread)):
            await interaction.channel.send(feedback)
        await interaction.followup.send("Kick-Befehl ausgeführt.", ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send("Ich kann dieses Mitglied nicht kicken (eventuell höhere Rolle).", ephemeral=True)

@bot.tree.command(name="ban", description="Bannt ein Mitglied für eine bestimmte Dauer und sendet einen DM-Countdown.")
@app_commands.describe(member="Das zu bannende Mitglied", duration="Dauer des Banns (z.B. 30m, 2h, 5d)", reason="Der Grund für den Bann")
@app_commands.checks.has_permissions(ban_members=True)
async def ban_command(interaction: Interaction, member: Member, duration: str, reason: str = "Kein Grund angegeben."):
    if not interaction.guild or not interaction.guild.me.guild_permissions.ban_members: return
    if member.id == interaction.user.id or member.id == bot.user.id:
        await interaction.response.send_message("Du kannst dich selbst oder den Bot nicht bannen!", ephemeral=True)
        return
    
    parsed_duration = parse_duration(duration)
    if not parsed_duration:
        await interaction.response.send_message("Ungültiges Zeitformat. Nutze z.B. `30m`, `2h`, `5d`.", ephemeral=True)
        return
        
    await ban_manager.start_ban(interaction, member, parsed_duration, reason)

# ===== List Commands =====
@bot.tree.command(name="unban", description="Entbannt ein Mitglied manuell und sendet einen Einladungslink.")
@app_commands.describe(reason="Der Grund für das manuelle Entbannen")
@app_commands.checks.has_permissions(ban_members=True)
async def unban_command(interaction: Interaction, reason: str = "Manuell entbannt."):
    if not interaction.guild:
        await interaction.response.send_message("Dieser Befehl kann nur auf einem Server verwendet werden.", ephemeral=True)
        return
    if not interaction.guild.me.guild_permissions.ban_members:
        await interaction.response.send_message("Ich benötige die Berechtigung 'Mitglieder bannen' zum Entbannen.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True) # Defer early as fetching bans can take time

    banned_users_entries = []
    async for ban_entry in interaction.guild.bans(limit=25):
        banned_users_entries.append(ban_entry)

    if not banned_users_entries:
        await interaction.followup.send("Es gibt keine gebannten Benutzer auf diesem Server, die in einer Liste angezeigt werden könnten.", ephemeral=True)
        return

    view = UnbanSelectView(ban_manager, bot, banned_users_entries, reason, interaction)
    await interaction.followup.send("Wähle einen Benutzer zum Entbannen:", view=view, ephemeral=True)

@bot.tree.command(name="list_start", description="Startet eine neue interaktive Teilnahmeliste in diesem Channel.")
@app_commands.checks.has_permissions(manage_channels=True, manage_roles=True)
async def list_start_command(interaction: Interaction):
    if not interaction.guild or not isinstance(interaction.channel, TextChannel): return
    guild_id = interaction.guild.id
    guild_data = list_manager._get_guild_list_data(guild_id)
    
    if guild_data["message_id"] and guild_data["channel_id"] and not guild_data.get("locked", False):
        try:
            old_channel = interaction.guild.get_channel(guild_data["channel_id"])
            if isinstance(old_channel, TextChannel):
                await old_channel.fetch_message(guild_data["message_id"])
                await interaction.response.send_message(f"Es gibt bereits eine aktive Liste in {old_channel.mention}.", ephemeral=True)
                return
        except discord.NotFound: pass
    
    await list_manager._ensure_role(interaction.guild, ROLE_LIST_IN_NAME)
    await list_manager._ensure_role(interaction.guild, ROLE_LIST_RESERVE_NAME)
    guild_data.update({"main": [], "reserve": [], "locked": False})
    
    content_str = list_manager.generate_list_content_string(interaction.guild)
    view = PersistentListView(list_manager, guild_id)
    await interaction.response.send_message("Erstelle Liste...", ephemeral=True)
    try:
        list_message = await interaction.channel.send(content=content_str, view=view)
        guild_data.update({"channel_id": interaction.channel.id, "message_id": list_message.id})
        list_manager.save_lists_data()
        await interaction.edit_original_response(content="✅ Interaktive Liste erstellt!")
    except Exception as e:
        await interaction.edit_original_response(content=f"Fehler beim Erstellen der Liste: {e}")

@bot.tree.command(name="list_lock", description="Sperrt die aktive Teilnahmeliste, entfernt Buttons und Rollen.")
@app_commands.checks.has_permissions(manage_channels=True, manage_roles=True)
async def list_lock_command(interaction: Interaction):
    if not interaction.guild: return
    guild_id = interaction.guild.id
    guild_data = list_manager._get_guild_list_data(guild_id)

    if guild_data.get("locked", False):
         await interaction.response.send_message("ℹ️ Diese Liste ist bereits gesperrt.", ephemeral=True)
         return
    if not guild_data.get("message_id") or not guild_data.get("channel_id"):
        await interaction.response.send_message("ℹ️ Keine aktive Liste zum Sperren gefunden.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True) 
    
    message_edit_success = False
    try:
        channel = interaction.guild.get_channel(guild_data["channel_id"])
        if isinstance(channel, TextChannel):
            message = await channel.fetch_message(guild_data["message_id"])
            guild_data["locked"] = True 
            new_content = list_manager.generate_list_content_string(interaction.guild)
            await message.edit(content=new_content, view=None)
            message_edit_success = True
    except Exception as e:
        print(f"[ListLock] Error editing message {guild_data.get('message_id')}: {e}")

    feedback = []
    for role_name in [ROLE_LIST_IN_NAME, ROLE_LIST_RESERVE_NAME]:
        role = discord.utils.get(interaction.guild.roles, name=role_name)
        if role:
            try:
                await role.delete(reason="Liste gesperrt")
                feedback.append(f"Rolle '{role_name}' gelöscht.")
            except Exception as e:
                feedback.append(f"Fehler beim Löschen der Rolle '{role_name}': {e}")
    
    guild_data.update({"locked": True, "main": [], "reserve": []})
    list_manager.save_lists_data()
    
    final_response_message = "🔒 Liste gesperrt." + (" Buttons entfernt." if message_edit_success else "")
    if feedback:
        final_response_message += "\n" + "\n".join(feedback)

    await interaction.followup.send(final_response_message, ephemeral=True)


@bot.tree.command(name="list_refresh", description="Aktualisiert die Anzeige der Teilnahmeliste manuell.")
@app_commands.checks.has_permissions(manage_messages=True)
async def list_refresh_command(interaction: Interaction):
    if not interaction.guild: return
    guild_id = interaction.guild.id
    guild_data = list_manager._get_guild_list_data(guild_id)
    if not guild_data["message_id"] or not guild_data["channel_id"]:
        await interaction.response.send_message("Es gibt keine aktive Liste zum Aktualisieren.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await list_manager.update_list_message(guild_id, interaction=interaction)
    await interaction.followup.send("🔄 Liste aktualisiert.", ephemeral=True)

# --- Telegram Commands ---
@bot.tree.command(name="telegram_customize_message", description="Customize the intro message before telegram data.")
@app_commands.describe(message="First sentence the bot should say")
async def turf_customize_message(interaction: Interaction, message: str):
    user_id = str(interaction.user.id)
    user_configs.setdefault(user_id, {})["custom_intro"] = message
    save_telegram_configs(user_configs)
    await interaction.response.send_message("\u2705 Custom message updated.", ephemeral=False)

@bot.tree.command(name="telegram_save_preset", description="Save a message formatting preset.")
@app_commands.describe(preset_name="Name of your preset")
async def turf_save_preset(interaction: Interaction, preset_name: str):
    user_id = str(interaction.user.id)
    msg_format = user_configs.get(user_id, {}).get("message_format", DEFAULT_PRESET)
    turf_presets.setdefault(user_id, {})[preset_name] = msg_format
    save_turf_presets(turf_presets)
    await interaction.response.send_message(f"\u2705 Preset '{preset_name}' saved.", ephemeral=True)

class PresetSelect(discord.ui.Select):
    def __init__(self, user_id_str: str):
        self.user_id_str = user_id_str
        options_dict = {DEFAULT_PRESET_NAME: DEFAULT_PRESET, **turf_presets.get(self.user_id_str, {})}
        options = [SelectOption(label=name, value=name) for name in options_dict.keys()] or [SelectOption(label="No presets available", value="_disabled", default=True)]
        super().__init__(placeholder="Choose a preset", options=options, min_values=1, max_values=1, disabled=(options[0].value == "_disabled"))
        self.presets_map = options_dict
    async def callback(self, interaction: Interaction):
        chosen_preset_name = self.values[0]
        if preset_content := self.presets_map.get(chosen_preset_name):
            user_configs.setdefault(self.user_id_str, {})["message_format"] = preset_content
            save_telegram_configs(user_configs)
            await interaction.response.send_message(f"\u2705 Preset '{chosen_preset_name}' geladen!", ephemeral=False)
        else:
            await interaction.response.send_message(f"❌ Preset '{chosen_preset_name}' nicht gefunden.", ephemeral=True)

class PresetView(discord.ui.View):
    def __init__(self, user_id_str: str):
        super().__init__(timeout=180)
        self.add_item(PresetSelect(user_id_str))
        self.message: discord.Message | None = None

    async def on_timeout(self):
        if not self.message: return
        for item in self.children:
            if isinstance(item, (ui.Button, ui.Select)):
                item.disabled = True
        try:
            await self.message.edit(view=self)
        except discord.NotFound:
            pass

@bot.tree.command(name="telegram_load_preset", description="Load a previously saved preset.")
async def turf_load_preset(interaction: Interaction):
    user_id_str = str(interaction.user.id)
    if not turf_presets.get(user_id_str): 
        await interaction.response.send_message("Es sind keine Presets verfügbar.", ephemeral=True)
        return
    view = PresetView(user_id_str)
    await interaction.response.send_message("Wähle ein Preset:", view=view, ephemeral=False)
    view.message = await interaction.original_response()

# ===== NEU HINZUGEFÜGTER BEFEHL VON V6 =====
@bot.tree.command(name="turf_edit_default_preset_message", description="Set the default intro line (e.g. before telegram war output).")
@app_commands.describe(message="The message shown above the formatted turf info")
async def turf_edit_default_preset_message(interaction: discord.Interaction, message: str):
    user_id = str(interaction.user.id)
    user_configs.setdefault(user_id, {})["custom_intro"] = message
    save_telegram_configs(user_configs)
    await interaction.response.send_message("✅ Standard-Intro-Nachricht aktualisiert.", ephemeral=False)

@bot.tree.command(name="telegram_user_files_clear", description="Löscht ALLE deine persönlichen Telegram-Daten vom Bot.")
async def telegram_user_files_clear(interaction: Interaction):
    await interaction.response.defer(ephemeral=True) # Sofort antworten, um Timeouts zu vermeiden
    user_id_str = str(interaction.user.id)
    summary_lines = [f"🧹 **Aufräumarbeiten für {interaction.user.mention} abgeschlossen:**"]

    # 1. Aktiven Telegram-Client trennen und entfernen
    if client := bot.telegram_clients.pop(user_id_str, None):
        if client.is_connected():
            await client.disconnect()
        summary_lines.append("✅ Aktive Telegram-Verbindung wurde getrennt.")
    else:
        summary_lines.append("👌 Keine aktive Telegram-Verbindung zum Trennen gefunden.")

    # 2. Zugehörigen Discord-Webhook löschen
    user_conf = user_configs.get(user_id_str)
    if user_conf and (webhook_url := user_conf.get("webhook_url")):
        try:
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(webhook_url, session=session)
                await webhook.delete()
                summary_lines.append("✅ Zugehöriger Discord-Webhook wurde gelöscht.")
        except (discord.NotFound, ValueError):
            summary_lines.append("ℹ️ Der Webhook war bereits gelöscht oder ungültig.")
        except Exception as e:
            summary_lines.append(f"❌ Fehler beim Löschen des Webhooks: {e}")
    else:
        summary_lines.append("👌 Kein Webhook in deiner Konfiguration gefunden.")

    # 3. Einträge aus Konfigurationsdateien entfernen
    if user_configs.pop(user_id_str, None):
        save_telegram_configs(user_configs)
        summary_lines.append(f"✅ Konfiguration aus `{TELEGRAM_CONFIG_FILE}` entfernt.")
    else:
        summary_lines.append(f"👌 Keine Konfiguration in `{TELEGRAM_CONFIG_FILE}` gefunden.")

    if turf_presets.pop(user_id_str, None):
        save_turf_presets(turf_presets)
        summary_lines.append(f"✅ Presets aus `{PRESET_FILE}` entfernt.")
    else:
        summary_lines.append(f"👌 Keine Presets in `{PRESET_FILE}` gefunden.")

    # 4. Telegram-Session-Datei löschen
    session_file = f"user_{user_id_str}.session"
    if os.path.exists(session_file):
        try:
            os.remove(session_file)
            summary_lines.append(f"✅ Telegram-Session-Datei (`{session_file}`) wurde gelöscht.")
        except OSError as e:
            summary_lines.append(f"❌ Fehler beim Löschen der Session-Datei: {e}")
    else:
        summary_lines.append("👌 Keine Session-Datei zum Löschen gefunden.")
    
    summary_lines.append("\nDu kannst nun den Befehl `/telegram_set_channel` erneut ausführen, um alles neu einzurichten.")
    await interaction.followup.send("\n".join(summary_lines), ephemeral=True)


@bot.tree.command(name="telegram_set_channel", description="Setzt den Discord-Channel für Telegram-Kriegswarnungen.")
@app_commands.describe(channel="Der Channel, in den die Warnungen gepostet werden.")
@app_commands.checks.has_permissions(manage_webhooks=True, manage_channels=True)
async def turf_set_channel(interaction: Interaction, channel: TextChannel):
    if not interaction.guild: return
    
    await interaction.response.defer(ephemeral=True)
    
    if not isinstance(interaction.user, Member): return
    user = interaction.user
    user_id_str = str(user.id)

    await interaction.followup.send(f"✅ Turf-Alarm-Channel auf {channel.mention} gesetzt. Ich sende dir jetzt eine DM für die Konfiguration.", ephemeral=True)
    
    if user_id_str in user_configs and user_configs[user_id_str].get("webhook_url"):
        user_configs[user_id_str]["guild_id"] = interaction.guild.id
        save_telegram_configs(user_configs)
        try:
            await user.send(f"Dein Alarm-Channel wurde auf {channel.mention} in '{interaction.guild.name}' aktualisiert. Versuche, die Verbindung wiederherzustellen...")
        except discord.Forbidden: pass
        await start_telegram_client(user_id_str, user, is_interactive_setup=True)
        return
        
    try:
        await user.send(f"Hallo! Wir richten jetzt deinen Telegram-Forwarder für den Server '{interaction.guild.name}' ein.")
        
        def check(m):
            return m.author.id == user.id and isinstance(m.channel, discord.DMChannel)

        await user.send("1️⃣ **Bitte gib deine Telegram API ID ein:** ```[https://my.telegram.org/auth](https://my.telegram.org/auth)```")
        api_id_msg = await bot.wait_for("message", check=check, timeout=300)
        
        await user.send("2️⃣ **Bitte gib deinen Telegram API Hash ein:**")
        api_hash_msg = await bot.wait_for("message", check=check, timeout=300)

        await user.send("3️⃣ **Bitte gib den exakten Telegram-Benutzernamen des Ziel-Chats/Bots ein (ohne @):**")
        telegram_user_msg = await bot.wait_for("message", check=check, timeout=300)

        await user.send("⚙️ Erstelle Webhook und speichere Konfiguration...")

        webhook_name = f"Turf Bot ({user.display_name})"
        try:
            webhooks = await channel.webhooks()
            webhook = discord.utils.get(webhooks, name=webhook_name)
            if not webhook:
                webhook = await channel.create_webhook(name=webhook_name)
        except discord.Forbidden:
            await user.send(f"❌ **Fehler:** Ich habe keine Berechtigung, einen Webhook im Channel `{channel.name}` zu erstellen. Bitte gib mir die 'Webhooks verwalten' Berechtigung und versuche es erneut.")
            return
        except Exception as e:
            await user.send(f"❌ **Fehler:** Ein unerwarteter Fehler beim Erstellen des Webhooks ist aufgetreten: {e}")
            return

        user_configs[user_id_str] = {
            "api_id": api_id_msg.content.strip(),
            "api_hash": api_hash_msg.content.strip(),
            "telegram_user": telegram_user_msg.content.strip(),
            "webhook_url": webhook.url,
            "guild_id": interaction.guild.id,
            "channel_id": channel.id
        }
        save_telegram_configs(user_configs)

        await user.send("✅ Konfiguration gespeichert! Fahre mit dem Telegram-Login fort...")
        await start_telegram_client(user_id_str, user, is_interactive_setup=True)

    except asyncio.TimeoutError:
        try:
            await user.send("⌛ Die Zeit für die Eingabe ist abgelaufen. Bitte starte den Befehl erneut.")
        except discord.Forbidden:
            pass
    except discord.Forbidden:
        print(f"[ERROR] Konnte keine DM an {user.name} senden, um die Einrichtung zu starten.")
    except Exception as e:
        print(f"[TelegramSetup] Kritischer Fehler für {user.name} ({user.id}): {e}")
        try:
            await user.send(f"Ein unerwarteter, kritischer Fehler ist aufgetreten: {e}")
        except discord.Forbidden:
            pass

# --- Other Commands ---
@bot.tree.command(name="move", description="Move a user between two voice channels repeatedly.")
@app_commands.describe(member="User to move", talk1="First VC", talk2="Second VC", delay="Seconds between moves")
@app_commands.checks.has_permissions(move_members=True)
async def move(interaction: Interaction, member: Member, talk1: VoiceChannel, talk2: VoiceChannel, delay: float):
    if not interaction.guild: return
    if delay < 0.1: 
        await interaction.response.send_message("Delay must be ≥ 0.1s", ephemeral=True); return
    
    task_key = (interaction.guild.id, member.id)
    if task_key in bot.moving_tasks:
        bot.moving_tasks[task_key].cancel()
    
    async def move_loop(guild_id: int, member_id: int, vc1_id: int, vc2_id: int, move_delay: float):
        current_vc_id = vc1_id
        try:
            while True:
                guild = bot.get_guild(guild_id)
                if not guild: break
                
                target_member = guild.get_member(member_id)
                if not target_member or not target_member.voice or not target_member.voice.channel:
                    print(f"[Move] Target {member_id} left voice. Stopping task.")
                    break
                
                channel_to_move_to = guild.get_channel(current_vc_id)
                if not isinstance(channel_to_move_to, VoiceChannel):
                    print(f"[Move] Channel {current_vc_id} not found or not a VC. Stopping.")
                    break

                try:
                    await target_member.move_to(channel_to_move_to)
                except discord.HTTPException as e:
                    print(f"[Move] Could not move member {member_id}: {e}")
                    break
                
                current_vc_id = vc2_id if current_vc_id == vc1_id else vc1_id
                
                await asyncio.sleep(move_delay)
        except asyncio.CancelledError:
            print(f"[Move] Task for member {member_id} was cancelled.")
        finally:
            print(f"[Move] Cleaning up task for member {member_id}.")
            bot.moving_tasks.pop((guild_id, member_id), None)
            
    task = asyncio.create_task(move_loop(interaction.guild.id, member.id, talk1.id, talk2.id, delay))
    bot.moving_tasks[task_key] = task
    await interaction.response.send_message(f"🚀 {member.display_name} wird bewegt.", ephemeral=False)

@bot.tree.command(name="stopmove", description="Stop moving a user.")
@app_commands.describe(member="The user to stop moving")
@app_commands.checks.has_permissions(move_members=True)
async def stopmove(interaction: Interaction, member: Member):
    if not interaction.guild: return
    task_key = (interaction.guild.id, member.id)
    if task := bot.moving_tasks.pop(task_key, None):
        task.cancel()
        await interaction.response.send_message(f"🛑 Bewegung für {member.display_name} gestoppt.", ephemeral=False)
    else:
        await interaction.response.send_message(f"{member.display_name} wird nicht bewegt.", ephemeral=True)

@bot.tree.command(name="reminder", description="Send a reminder DM to a user.")
@app_commands.describe(user="User to remind")
async def reminder(interaction: Interaction, user: User):
    if not interaction.guild: return
    message_to_send = bot.reminder_messages.get(interaction.guild.id, "This is your reminder!")
    try:
        await user.send(f"📌 **Erinnerung von '{interaction.guild.name}'**:\n> {message_to_send}")
        if interaction.response.is_done():
            await interaction.followup.send(f"Erinnerung an {user.mention} gesendet.", ephemeral=False)
        else:
            await interaction.response.send_message(f"Erinnerung an {user.mention} gesendet.", ephemeral=False)
    except discord.Forbidden:
        if interaction.response.is_done():
            await interaction.followup.send(f"Konnte {user.mention} keine DM senden.", ephemeral=False)
        else:
            await interaction.response.send_message(f"Konnte {user.mention} keine DM senden.", ephemeral=False)

@bot.tree.command(name="reminder_edit", description="Edit the reminder message for this server.")
@app_commands.describe(message="The new message to send")
@app_commands.checks.has_permissions(manage_guild=True)
async def reminder_edit(interaction: Interaction, message: str):
    if not interaction.guild: return
    bot.reminder_messages[interaction.guild.id] = message
    save_reminder_messages(bot.reminder_messages)
    await interaction.response.send_message("✏️ Erinnerungsnachricht aktualisiert.", ephemeral=False)

@bot.tree.command(name="restart", description="Starte den Bot neu (nur für Owner).")
async def restart_command(interaction: Interaction):
    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("🚫 Du bist nicht berechtigt.", ephemeral=True); return
    await interaction.response.defer(ephemeral=False)
    save_reminder_messages(bot.reminder_messages) 
    list_manager.save_lists_data()
    for client in bot.telegram_clients.values():
        if client.is_connected():
            await client.disconnect()
    
    restart_info = {"channel_id": None, "message_id": None}
    if interaction.channel and isinstance(interaction.channel, (TextChannel, discord.Thread)):
        restart_info["channel_id"] = interaction.channel.id
        try:
            msg = await interaction.followup.send("🔁 Bot wird neu gestartet...")
            if msg:
                restart_info["message_id"] = msg.id
        except Exception as e: 
            print(f"[Restart] Could not send restart message: {e}")
        
    with open("restart_info.json", "w") as f: json.dump(restart_info, f)
    os.execv(sys.executable, ['python'] + sys.argv)

@bot.tree.command(name="message_role", description="Send a direct message to all users with a specific role.")
@app_commands.describe(role="The role to message", message="The message to send to each member")
@app_commands.checks.has_permissions(mention_everyone=True)
async def message_role(interaction: Interaction, role: discord.Role, message: str):
    if not interaction.guild: return
    await interaction.response.defer(ephemeral=True) 
    failed_to_dm, sent_count = [], 0
    header = f"**Nachricht von '{interaction.guild.name}' (Admin: {interaction.user.display_name}):**\n\n"
    
    for member in role.members:
        if member.bot: continue
        try: 
            await member.send(header + message)
            sent_count += 1
            await asyncio.sleep(0.3) 
        except Exception as e:
            failed_to_dm.append(f"{member.display_name} (Fehler: {type(e).__name__})")
            
    summary = f"✅ Nachricht an {sent_count} von {len(role.members)} Mitgliedern gesendet."
    if failed_to_dm:
        summary += f"\n❌ Konnte {len(failed_to_dm)} Mitgliedern keine DM senden."
    await interaction.followup.send(summary, ephemeral=False)

# ===== ON READY / Events =====
@bot.event
async def on_ready():
    print(f"Bot is starting up as {bot.user.name}...")
    
    bot.add_view(BanDMView(ban_manager))

    await list_manager.initialize_lists_on_ready()
    await ban_manager.initialize_sessions_on_ready()

    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"✅ Slash Commands synced: {len(synced)} commands")
    except Exception as e:
        print(f"❌ Fehler beim Sync der Slash Commands: {e}")
    
    print("[on_ready] Starte Telegram-Clients...")
    for user_id_str in list(user_configs.keys()):
        try:
            user_obj = await bot.fetch_user(int(user_id_str))
            await start_telegram_client(user_id_str, user_obj, is_interactive_setup=False)
        except (ValueError, discord.NotFound):
             await start_telegram_client(user_id_str, None, is_interactive_setup=False)
        except Exception as e:
            print(f"[ERROR] Fehler beim Starten des Telegram-Clients für User ID {user_id_str}: {e}")
            
    if os.path.exists("restart_info.json"):
        try:
            with open("restart_info.json", "r") as f:
                data = json.load(f)
            if (channel_id := data.get("channel_id")) and (channel := await bot.fetch_channel(channel_id)):
                if isinstance(channel, TextChannel) and (message_id := data.get("message_id")):
                    try:
                        msg = await channel.fetch_message(message_id)
                        await msg.edit(content="✅ Bot ist wieder online!")
                    except discord.NotFound:
                        pass
        except Exception as e:
            print(f"[Restart Confirm] Fehler: {e}")
        finally:
             if os.path.exists("restart_info.json"):
                os.remove("restart_info.json")

    print("[on_ready] Starte Hintergrund-Tasks...")
    asyncio.create_task(daily_telegram_notice())
    asyncio.create_task(auto_restart_timer())
    print("[on_ready] Alle Startvorgänge abgeschlossen.")

@bot.event
async def on_member_join(member: Member):
    ban_key = (member.guild.id, member.id)
    ban_entry = ban_manager.active_bans.get(ban_key)

    if ban_entry and ban_entry.get("status") == "unbanned_pending_roles":
        roles_to_restore_ids = ban_entry.get("roles_to_restore")
        if roles_to_restore_ids:
            print(f"[on_member_join] User {member.display_name} ({member.id}) ist Guild {member.guild.name} beigetreten. Versuche Rollenwiederherstellung.")
            await ban_manager._restore_roles(member, roles_to_restore_ids)
        
        ban_manager.active_bans.pop(ban_key, None)
        ban_manager.save_bans()
        print(f"[on_member_join] Bann-Eintrag für {member.id} nach Rejoin und Rollenversuch entfernt.")

# ===== Auto Restart Logic =====
async def auto_restart_timer():
    await bot.wait_until_ready() 
    await asyncio.sleep(1800
    print("🔁 [AutoRestart] Timer abgelaufen. Starte neu...")
    save_reminder_messages(bot.reminder_messages)
    list_manager.save_lists_data()
    for client in bot.telegram_clients.values():
        if client.is_connected():
            await client.disconnect()
    os.execv(sys.executable, ['python'] + sys.argv)

# ===== RUN BOT =====
if __name__ == "__main__":
    try:
        bot.run(DISCORD_TOKEN)
    except discord.LoginFailure:
        print("[FATAL] Login failed: Invalid Discord token.")
    except Exception as e:
        print(f"[FATAL] An error occurred while running the bot: {e}")