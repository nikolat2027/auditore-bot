import os
import json
import time
import re
import random
import threading
from typing import Dict, Set, List, Optional
from datetime import datetime
from dotenv import load_dotenv
import requests

# ===== ЗАГРУЗКА КОНФИГУРАЦИИ ИЗ .env =====
load_dotenv()

VK_TOKEN = os.getenv("VK_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID", 0))
OWNER_ID = int(os.getenv("OWNER_ID", 0))

if not VK_TOKEN or GROUP_ID == 0 or OWNER_ID == 0:
    raise ValueError("Не заданы переменные окружения: VK_TOKEN, GROUP_ID, OWNER_ID")

API_VERSION = "5.199"
DATA_FILE = "data.json"
MAX_LOG_ENTRIES = 200
CLEAR_LIMIT = 50
CACHE_TTL = 60
NAME_CACHE_TTL = 300
SAVE_DELAY = 5.0
PENDING_TIMEOUT = 300
INACTIVE_DAYS = 14
INACTIVE_SECONDS = INACTIVE_DAYS * 24 * 3600
CLEANUP_INTERVAL = 3600 * 24
DUPLICATE_CACHE_TTL = 10  # секунд, в течение которых игнорируем повторные сообщения

# ===== КЕШ ДЛЯ API И ДУБЛЕЙ =====
members_cache = {}
name_cache = {}
processed_messages = {}  # {(peer_id, conversation_message_id): timestamp}

# ===== РАБОТА С ДАННЫМИ =====
class BotData:
    def __init__(self):
        self.admins: Set[int] = set()
        self.warns: Dict[int, Dict[int, int]] = {}
        self.active_chats: Set[int] = set()
        self.names: Dict[int, str] = {}
        self.join_dates: Dict[int, Dict[int, int]] = {}
        self.greetings: Dict[int, str] = {}
        self.logs: List[Dict] = []
        self.profiles: Dict[int, Dict[int, Dict[str, str]]] = {}
        self.form_templates: Dict[int, List[str]] = {}
        self.ideas: List[Dict] = []
        self.pending_actions: Dict[int, Dict] = {}
        self.ideas_enabled: bool = True
        self.last_activity: Dict[int, int] = {}
        self.self_exited: Dict[int, Set[int]] = {}

        self._dirty = False
        self._save_timer = None
        self._lock = threading.Lock()

    def load(self):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.admins = set(data.get("admins", []))
                self.warns = {int(k): {int(u): c for u, c in v.items()}
                              for k, v in data.get("warns", {}).items()}
                self.active_chats = set(data.get("active_chats", []))
                self.names = {int(k): v for k, v in data.get("names", {}).items()}
                self.join_dates = {int(k): {int(u): d for u, d in v.items()}
                                   for k, v in data.get("join_dates", {}).items()}
                self.greetings = {int(k): v for k, v in data.get("greetings", {}).items()}
                self.logs = data.get("logs", [])
                self.profiles = {int(k): {int(u): v for u, v in prof.items()}
                                 for k, prof in data.get("profiles", {}).items()}
                self.form_templates = {int(k): v for k, v in data.get("form_templates", {}).items()}
                self.ideas = data.get("ideas", [])
                pending = data.get("pending_actions", {})
                self.pending_actions = {}
                for k, v in pending.items():
                    self.pending_actions[int(k)] = v
                self.ideas_enabled = data.get("ideas_enabled", True)
                last_act = data.get("last_activity", {})
                self.last_activity = {int(k): v for k, v in last_act.items()}
                self_exited = data.get("self_exited", {})
                self.self_exited = {int(k): set(v) for k, v in self_exited.items()}
        except FileNotFoundError:
            pass

    def _do_save(self):
        with self._lock:
            if not self._dirty:
                return
            data = {
                "admins": list(self.admins),
                "warns": self.warns,
                "active_chats": list(self.active_chats),
                "names": self.names,
                "join_dates": self.join_dates,
                "greetings": self.greetings,
                "logs": self.logs[-MAX_LOG_ENTRIES:],
                "profiles": self.profiles,
                "form_templates": self.form_templates,
                "ideas": self.ideas,
                "pending_actions": self.pending_actions,
                "ideas_enabled": self.ideas_enabled,
                "last_activity": self.last_activity,
                "self_exited": {str(k): list(v) for k, v in self.self_exited.items()},
            }
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self._dirty = False
            self._save_timer = None

    def save(self):
        with self._lock:
            self._dirty = True
            if self._save_timer is None:
                self._save_timer = threading.Timer(SAVE_DELAY, self._do_save)
                self._save_timer.start()

    def add_log(self, action: str, target_id: int, initiator_id: int, peer_id: int, comment: str = ""):
        entry = {
            "timestamp": int(time.time()),
            "action": action,
            "target_id": target_id,
            "initiator_id": initiator_id,
            "peer_id": peer_id,
            "comment": comment
        }
        self.logs.append(entry)
        self.save()

    def update_activity(self, peer_id: int):
        if peer_id in self.active_chats:
            self.last_activity[peer_id] = int(time.time())
            self.save()

    def add_idea(self, author_id: int, text: str):
        idea = {
            "id": len(self.ideas) + 1,
            "author_id": author_id,
            "text": text,
            "votes": [],
            "date": int(time.time()),
            "closed": False
        }
        self.ideas.append(idea)
        self.save()
        return idea

    def vote_idea(self, idea_id: int, user_id: int) -> bool:
        for idea in self.ideas:
            if idea["id"] == idea_id and not idea["closed"]:
                if user_id not in idea["votes"]:
                    idea["votes"].append(user_id)
                    self.save()
                    return True
                else:
                    return False
        return False

    def close_idea(self, idea_id: int) -> bool:
        for idea in self.ideas:
            if idea["id"] == idea_id:
                idea["closed"] = True
                self.save()
                return True
        return False

    def delete_idea(self, idea_id: int) -> bool:
        for i, idea in enumerate(self.ideas):
            if idea["id"] == idea_id:
                del self.ideas[i]
                self.save()
                return True
        return False

    def get_open_ideas(self):
        return [i for i in self.ideas if not i["closed"]]

    def get_all_ideas(self):
        return self.ideas

    def toggle_ideas(self):
        self.ideas_enabled = not self.ideas_enabled
        self.save()
        return self.ideas_enabled

    def clear_chat_data(self, peer_id: int):
        if peer_id in self.active_chats:
            self.active_chats.remove(peer_id)
        self.warns.pop(peer_id, None)
        self.greetings.pop(peer_id, None)
        self.form_templates.pop(peer_id, None)
        self.profiles.pop(peer_id, None)
        self.join_dates.pop(peer_id, None)
        self.last_activity.pop(peer_id, None)
        self.self_exited.pop(peer_id, None)
        self.logs = [log for log in self.logs if log.get("peer_id") != peer_id]
        self.save()

# ===== ВЫЗОВЫ VK API =====
def vk_api(method: str, params: dict, retries: int = 3) -> dict:
    url = f"https://api.vk.com/method/{method}"
    params["access_token"] = VK_TOKEN
    params["v"] = API_VERSION
    for attempt in range(retries):
        try:
            resp = requests.post(url, data=params, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"Ошибка HTTP {resp.status_code} при вызове {method}")
        except requests.exceptions.RequestException as e:
            print(f"Попытка {attempt+1} не удалась: {e}")
            time.sleep(2 ** attempt)
    return {"error": {"error_msg": "Превышено число попыток"}}

def send_message(peer_id: int, text: str, keyboard: dict = None):
    params = {
        "peer_id": peer_id,
        "message": text,
        "random_id": random.randint(1, 2**31)
    }
    if keyboard:
        params["keyboard"] = json.dumps(keyboard)
    vk_api("messages.send", params)

def kick_from_chat(peer_id: int, user_id: int):
    chat_id = peer_id - 2000000000
    vk_api("messages.removeChatUser", {
        "chat_id": chat_id,
        "user_id": user_id
    })

def get_chat_members_cached(peer_id: int) -> List[dict]:
    now = time.time()
    if peer_id in members_cache and now - members_cache[peer_id]['time'] < CACHE_TTL:
        return members_cache[peer_id]['data']
    resp = vk_api("messages.getConversationMembers", {"peer_id": peer_id})
    items = resp.get("response", {}).get("items", [])
    members_cache[peer_id] = {'data': items, 'time': now}
    return items

def is_bot_admin(peer_id: int) -> bool:
    items = get_chat_members_cached(peer_id)
    for item in items:
        if item.get("member_id") == -GROUP_ID:
            if item.get("is_admin") or item.get("is_owner"):
                return True
    return False

def is_user_chat_admin(peer_id: int, user_id: int) -> bool:
    items = get_chat_members_cached(peer_id)
    for item in items:
        if item.get("member_id") == user_id:
            if item.get("is_admin") or item.get("is_owner"):
                return True
    return False

def delete_messages(peer_id: int, message_ids: List[int]):
    if not message_ids:
        return
    vk_api("messages.delete", {
        "peer_id": peer_id,
        "conversation_message_ids": message_ids,
        "delete_for_all": 1
    })

def get_last_messages(peer_id: int, count: int = 50) -> List[dict]:
    resp = vk_api("messages.getHistory", {
        "peer_id": peer_id,
        "count": count
    })
    if "response" in resp:
        return resp["response"]["items"]
    return []

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
def extract_user_id(args: List[str]) -> Optional[int]:
    if not args:
        return None
    text = " ".join(args)
    match = re.search(r"\[id(\d+)\|", text)
    if match:
        return int(match.group(1))
    if text.isdigit():
        return int(text)
    return None

def get_user_name_cached(user_id: int, data: BotData) -> str:
    if user_id in data.names:
        return data.names[user_id]
    now = time.time()
    if user_id in name_cache and now - name_cache[user_id]['time'] < NAME_CACHE_TTL:
        return name_cache[user_id]['name']
    resp = vk_api("users.get", {"user_ids": user_id})
    if "response" in resp and resp["response"]:
        user = resp["response"][0]
        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        if name:
            data.names[user_id] = name
            data.save()
            name_cache[user_id] = {'name': name, 'time': now}
            return name
    return f"id{user_id}"

def get_user_display(user_id: int, data: BotData, peer_id: int = None) -> str:
    if peer_id is not None and peer_id in data.profiles and user_id in data.profiles[peer_id]:
        profile = data.profiles[peer_id][user_id]
        for key in profile.keys():
            if key.lower() == "ник":
                return f"[id{user_id}|{profile[key]}]"
    name = get_user_name_cached(user_id, data)
    return f"[id{user_id}|{name}]"

def get_long_poll_server():
    resp = vk_api("groups.getLongPollServer", {"group_id": GROUP_ID})
    if "response" in resp:
        return resp["response"]
    else:
        print("Ошибка получения Long Poll сервера:", resp)
        return None

def format_timestamp(ts: int) -> str:
    if not ts:
        return "неизвестно"
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%d.%m.%Y %H:%M")

# ===== КЛАВИАТУРА ДЛЯ ЛИЧНЫХ СООБЩЕНИЙ =====
def get_private_keyboard() -> dict:
    return {
        "one_time": False,
        "buttons": [
            [{"action": {"type": "text", "label": "🆘 Обратиться в поддержку"}, "color": "default"}]
        ]
    }

# ===== ОБРАБОТЧИК ЛИЧНЫХ СООБЩЕНИЙ =====
def handle_private_message(peer_id: int, from_id: int, text: str, data: BotData):
    if peer_id != from_id:
        return

    if peer_id in data.pending_actions:
        pending = data.pending_actions[peer_id]
        if time.time() - pending.get("timestamp", 0) > PENDING_TIMEOUT:
            del data.pending_actions[peer_id]
            data.save()
            send_message(peer_id, "⏰ Время ожидания истекло. Если хотите обратиться в поддержку, нажмите кнопку снова.",
                         keyboard=get_private_keyboard())
            return

        if pending["action"] == "support":
            if not text or text.strip() == "":
                send_message(peer_id, "❗ Опишите вашу проблему текстом. Сообщение не может быть пустым.")
                return
            send_message(peer_id, "✅ Ваше обращение одобрено. Мы рассмотрим его в ближайшее время.",
                         keyboard=get_private_keyboard())
            del data.pending_actions[peer_id]
            data.save()
            return

    if text.lower() == "/start" or text.lower() == "старт":
        send_message(peer_id,
                     "🤖 Бот работает исключительно в беседах.\n"
                     "Для обращения в поддержку нажмите кнопку ниже.",
                     keyboard=get_private_keyboard())
        return

    if text == "🆘 Обратиться в поддержку":
        data.pending_actions[peer_id] = {"action": "support", "timestamp": time.time()}
        data.save()
        send_message(peer_id, "📝 Напишите описание вашей проблемы одним сообщением.",
                     keyboard=None)
        return

    send_message(peer_id,
                 "❓ Я вас не понимаю. Нажмите кнопку «Обратиться в поддержку» или введите /start.",
                 keyboard=get_private_keyboard())

# ===== ОБРАБОТЧИКИ КОМАНД В БЕСЕДАХ =====

def handle_start(peer_id: int, from_id: int, args: List[str], data: BotData):
    if not is_bot_admin(peer_id):
        send_message(peer_id, "❗ В настройках беседы назначьте бота как администратора, затем напишите /start.")
        return
    if peer_id in data.active_chats:
        send_message(peer_id, "ℹ️ Бот уже запущен в данной беседе.")
    else:
        data.active_chats.add(peer_id)
        data.update_activity(peer_id)
        send_message(peer_id, "✅ Вы успешно запустили бота!")

def handle_setnick(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if len(args) < 2:
        send_message(peer_id, "❗ Используйте: /setnick @user <ник>")
        return
    target_id = extract_user_id([args[0]])
    if target_id is None:
        send_message(peer_id, "❗ Не удалось распознать пользователя. Используйте @упоминание.")
        return
    nickname = " ".join(args[1:])
    if not nickname:
        send_message(peer_id, "❗ Укажите ник.")
        return

    if peer_id not in data.profiles:
        data.profiles[peer_id] = {}
    if target_id not in data.profiles[peer_id]:
        data.profiles[peer_id][target_id] = {}
    data.profiles[peer_id][target_id]["Ник"] = nickname

    data.add_log("установил ник", target_id, from_id, peer_id, nickname)
    send_message(peer_id, f"✅ Ник для {get_user_display(target_id, data, peer_id)} установлен: {nickname}")
    data.save()

def handle_rnick(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if not args:
        send_message(peer_id, "❗ Используйте: /rnick @user")
        return
    target_id = extract_user_id(args)
    if target_id is None:
        send_message(peer_id, "❗ Не удалось распознать пользователя. Используйте @упоминание.")
        return

    removed = False
    if peer_id in data.profiles and target_id in data.profiles[peer_id]:
        profile = data.profiles[peer_id][target_id]
        nick_key = None
        for key in profile.keys():
            if key.lower() == "ник":
                nick_key = key
                break
        if nick_key is not None:
            old_nick = profile[nick_key]
            del profile[nick_key]
            if not profile:
                del data.profiles[peer_id][target_id]
                if not data.profiles[peer_id]:
                    del data.profiles[peer_id]
            data.add_log("удалил ник", target_id, from_id, peer_id, old_nick)
            send_message(peer_id, f"✅ Ник у {get_user_display(target_id, data, peer_id)} удалён.")
            removed = True

    if not removed:
        send_message(peer_id, f"⚠️ У пользователя {get_user_display(target_id, data, peer_id)} нет ника.")
    else:
        data.save()

def handle_kick(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if not args:
        send_message(peer_id, "❗ Используйте: /kick @user [причина: текст]")
        return
    target_id = extract_user_id([args[0]])
    if target_id is None:
        send_message(peer_id, "❗ Не удалось распознать пользователя. Используйте @упоминание.")
        return
    if target_id in data.admins:
        data.admins.remove(target_id)
        data.save()
    reason = " ".join(args[1:]) if len(args) > 1 else ""
    if reason.lower().startswith("причина:"):
        reason = reason[8:].strip()
        if not reason:
            reason = "(причина не указана)"
    kick_from_chat(peer_id, target_id)
    data.add_log("кикнул", target_id, from_id, peer_id, reason)
    kicker_display = get_user_display(from_id, data, peer_id)
    target_display = get_user_display(target_id, data, peer_id)
    if reason:
        text = f"{kicker_display} кикнул {target_display} по причине: {reason}"
    else:
        text = f"{kicker_display} кикнул {target_display}."
    send_message(peer_id, text)

def handle_allkick(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    target_id = extract_user_id(args)
    if not target_id:
        send_message(peer_id, "❗ Укажите пользователя: /allkick @user")
        return
    for chat in data.active_chats.copy():
        try:
            kick_from_chat(chat, target_id)
        except:
            pass
    data.add_log("исключил из всех бесед", target_id, from_id, peer_id, "")
    send_message(peer_id, f"{get_user_display(from_id, data, peer_id)} исключил {get_user_display(target_id, data, peer_id)} из всех бесед.")

def handle_admin(peer_id: int, from_id: int, args: List[str], data: BotData):
    if not is_user_chat_admin(peer_id, from_id):
        send_message(peer_id, "⛔ Команда доступна только администраторам беседы.")
        return

    if not args:
        if data.admins:
            lines = [get_user_display(uid, data, peer_id) for uid in data.admins]
            text = "👑 Список администраторов бота:\n" + "\n".join(lines)
        else:
            text = "👑 Список администраторов пуст."
        send_message(peer_id, text)
        return

    if args[0].lower() == "remove":
        if len(args) < 2:
            send_message(peer_id, "❗ Укажите пользователя: /admin remove @user")
            return
        target_id = extract_user_id(args[1:])
        if target_id is None:
            send_message(peer_id, "❗ Не удалось распознать пользователя.")
            return
        if target_id not in data.admins:
            send_message(peer_id, f"⚠️ Пользователь {get_user_display(target_id, data, peer_id)} не является администратором.")
            return
        data.admins.remove(target_id)
        data.add_log("снял статус администратора", target_id, from_id, peer_id, "")
        send_message(peer_id, f"✅ Статус администратора у {get_user_display(target_id, data, peer_id)} снят.")
        data.save()
    else:
        target_id = extract_user_id(args)
        if target_id is None:
            send_message(peer_id, "❗ Не удалось распознать пользователя. Используйте @упоминание.")
            return
        if target_id in data.admins:
            send_message(peer_id, f"⚠️ Пользователь {get_user_display(target_id, data, peer_id)} уже является администратором.")
            return
        data.admins.add(target_id)
        data.add_log("назначил администратором", target_id, from_id, peer_id, "")
        send_message(peer_id, f"✅ {get_user_display(target_id, data, peer_id)} назначен администратором.")
        data.save()

def handle_warn(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    target_id = extract_user_id(args)
    if not target_id:
        send_message(peer_id, "❗ Укажите пользователя: /warn @user")
        return
    if peer_id not in data.warns:
        data.warns[peer_id] = {}
    current = data.warns[peer_id].get(target_id, 0)
    if current >= 2:
        kick_from_chat(peer_id, target_id)
        del data.warns[peer_id][target_id]
        data.add_log("получил 3 предупреждения и кикнут", target_id, from_id, peer_id, "")
        send_message(peer_id, f"⚠️ {get_user_display(target_id, data, peer_id)} получил 3-е предупреждение и исключён.")
    else:
        data.warns[peer_id][target_id] = current + 1
        data.add_log("получил предупреждение", target_id, from_id, peer_id, f"{current+1}/3")
        send_message(peer_id, f"⚠️ {get_user_display(target_id, data, peer_id)} выдано предупреждение ({current+1}/3).")
    data.save()

def handle_unwarn(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    target_id = extract_user_id(args)
    if not target_id:
        send_message(peer_id, "❗ Укажите пользователя: /unwarn @user")
        return
    if peer_id not in data.warns or target_id not in data.warns[peer_id]:
        send_message(peer_id, f"⚠️ У {get_user_display(target_id, data, peer_id)} нет предупреждений.")
        return
    current = data.warns[peer_id][target_id]
    if current <= 0:
        send_message(peer_id, f"⚠️ У {get_user_display(target_id, data, peer_id)} уже 0 предупреждений.")
        return
    data.warns[peer_id][target_id] = current - 1
    data.add_log("снято предупреждение", target_id, from_id, peer_id, f"{current-1}/3")
    send_message(peer_id, f"✅ У {get_user_display(target_id, data, peer_id)} снято одно предупреждение (осталось {current-1}/3).")
    data.save()

def handle_allunwarn(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    target_id = extract_user_id(args)
    if not target_id:
        send_message(peer_id, "❗ Укажите пользователя: /allunwarn @user")
        return
    if peer_id not in data.warns or target_id not in data.warns[peer_id]:
        send_message(peer_id, f"⚠️ У {get_user_display(target_id, data, peer_id)} нет предупреждений.")
        return
    del data.warns[peer_id][target_id]
    data.add_log("сняты все предупреждения", target_id, from_id, peer_id, "")
    send_message(peer_id, f"✅ Все предупреждения у {get_user_display(target_id, data, peer_id)} сняты.")
    data.save()

def handle_warnlist(peer_id: int, from_id: int, args: List[str], data: BotData):
    warns_dict = data.warns.get(peer_id, {})
    if not warns_dict:
        send_message(peer_id, "📋 В этой беседе нет предупреждений.")
        return
    lines = [f"{get_user_display(uid, data, peer_id)}: {cnt}/3" for uid, cnt in warns_dict.items()]
    send_message(peer_id, "📋 Список предупреждений:\n" + "\n".join(lines))

def handle_online(peer_id: int, from_id: int, args: List[str], data: BotData):
    members = get_chat_members_cached(peer_id)
    if not members:
        send_message(peer_id, "Не удалось получить список участников.")
        return
    online_users = []
    for member in members:
        user_id = member.get("member_id")
        if user_id and user_id > 0 and member.get("is_online", False):
            online_users.append(user_id)
    if not online_users:
        send_message(peer_id, "🟢 В беседе нет пользователей онлайн.")
        return
    lines = [get_user_display(uid, data, peer_id) for uid in online_users[:20]]
    text = "🟢 Пользователи онлайн:\n" + "\n".join(lines)
    if len(online_users) > 20:
        text += f"\n... и ещё {len(online_users)-20} человек."
    send_message(peer_id, text)

def handle_reg(peer_id: int, from_id: int, args: List[str], data: BotData):
    if not args:
        send_message(peer_id, "❗ Укажите пользователя: /reg @user")
        return
    target_id = extract_user_id(args)
    if target_id is None:
        send_message(peer_id, "❗ Не удалось распознать пользователя.")
        return
    join_dates = data.join_dates.get(peer_id, {})
    if target_id not in join_dates:
        send_message(peer_id, f"⚠️ Дата добавления {get_user_display(target_id, data, peer_id)} неизвестна.")
        return
    timestamp = join_dates[target_id]
    date_str = format_timestamp(timestamp)
    send_message(peer_id, f"📅 {get_user_display(target_id, data, peer_id)} добавлен в беседу: {date_str}")

def handle_regall(peer_id: int, from_id: int, args: List[str], data: BotData):
    members = get_chat_members_cached(peer_id)
    if not members:
        send_message(peer_id, "Не удалось получить список участников.")
        return
    join_dates = data.join_dates.get(peer_id, {})
    lines = []
    for member in members:
        user_id = member.get("member_id")
        if user_id and user_id > 0:
            ts = join_dates.get(user_id)
            date_str = format_timestamp(ts) if ts else "неизвестно"
            lines.append(f"{get_user_display(user_id, data, peer_id)} — {date_str}")
    if not lines:
        send_message(peer_id, "📋 В этой беседе нет данных о добавлении.")
        return
    text = "📅 Список участников с датами добавления:\n" + "\n".join(lines[:20])
    if len(lines) > 20:
        text += f"\n... и ещё {len(lines)-20} человек."
    send_message(peer_id, text)

def handle_greeting(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if not args:
        if peer_id in data.greetings:
            del data.greetings[peer_id]
            data.save()
            send_message(peer_id, "✅ Приветствие удалено.")
        else:
            send_message(peer_id, "⚠️ Приветствие не было установлено.")
        return
    text = " ".join(args)
    if text.strip() == "":
        if peer_id in data.greetings:
            del data.greetings[peer_id]
            data.save()
            send_message(peer_id, "✅ Приветствие удалено.")
        else:
            send_message(peer_id, "⚠️ Приветствие не было установлено.")
    else:
        data.greetings[peer_id] = text
        data.add_log("установил приветствие", from_id, from_id, peer_id, text)
        send_message(peer_id, f"✅ Приветствие установлено:\n{text}")
        data.save()

def handle_template(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if not args:
        if peer_id in data.form_templates:
            del data.form_templates[peer_id]
            data.save()
            send_message(peer_id, "✅ Шаблон формы удалён.")
        else:
            send_message(peer_id, "⚠️ Шаблон не был установлен.")
        return
    template_text = " ".join(args)
    fields = [f.strip() for f in template_text.split("|") if f.strip()]
    if not fields:
        send_message(peer_id, "❗ Укажите поля, разделённые | (например: /шаблон Ник | Должность | Маска)")
        return
    data.form_templates[peer_id] = fields
    data.add_log("установил шаблон формы", from_id, from_id, peer_id, " | ".join(fields))
    send_message(peer_id, f"✅ Шаблон формы установлен:\n" + "\n".join([f"{i+1}. {f}" for i, f in enumerate(fields)]))
    data.save()

def handle_form(peer_id: int, from_id: int, args: List[str], data: BotData):
    if peer_id not in data.active_chats:
        send_message(peer_id, "❌ Бот не активен в этой беседе. Используйте /start.")
        return
    if peer_id not in data.form_templates:
        send_message(peer_id, "⚠️ В этой беседе не задан шаблон формы. Обратитесь к администратору.")
        return
    if not args:
        send_message(peer_id, f"❗ Укажите значения через |. Шаблон: {', '.join(data.form_templates[peer_id])}")
        return
    form_text = " ".join(args)
    values = [v.strip() for v in form_text.split("|") if v.strip()]
    template = data.form_templates[peer_id]
    if len(values) != len(template):
        send_message(peer_id, f"❗ Количество значений ({len(values)}) не совпадает с шаблоном ({len(template)}). Шаблон: {', '.join(template)}")
        return
    if peer_id not in data.profiles:
        data.profiles[peer_id] = {}
    data.profiles[peer_id][from_id] = dict(zip(template, values))
    data.add_log("заполнил анкету", from_id, from_id, peer_id, " | ".join(values))
    send_message(peer_id, "✅ Анкета успешно заполнена!")
    data.save()

def handle_nonick(peer_id: int, from_id: int, args: List[str], data: BotData):
    members = get_chat_members_cached(peer_id)
    if not members:
        send_message(peer_id, "Не удалось получить список участников.")
        return
    no_nick = []
    for member in members:
        user_id = member.get("member_id")
        if not user_id or user_id < 0:
            continue
        has_nick = False
        if peer_id in data.profiles and user_id in data.profiles[peer_id]:
            profile = data.profiles[peer_id][user_id]
            for key in profile.keys():
                if key.lower() == "ник":
                    has_nick = True
                    break
        if not has_nick:
            no_nick.append(user_id)

    if not no_nick:
        send_message(peer_id, "✅ У всех участников есть ники.")
        return
    lines = [get_user_display(uid, data, peer_id) for uid in no_nick[:20]]
    text = "📋 Участники без ника:\n" + "\n".join(lines)
    if len(no_nick) > 20:
        text += f"\n... и ещё {len(no_nick)-20} человек."
    send_message(peer_id, text)

def handle_nlist(peer_id: int, from_id: int, args: List[str], data: BotData):
    members = get_chat_members_cached(peer_id)
    if not members:
        send_message(peer_id, "Не удалось получить список участников.")
        return

    display_list = []
    for member in members:
        user_id = member.get("member_id")
        if not user_id or user_id < 0:
            continue
        # Исключаем самого пользователя, чтобы не показывать его в списке
        if user_id == from_id:
            continue
        has_nick = False
        if peer_id in data.profiles and user_id in data.profiles[peer_id]:
            profile = data.profiles[peer_id][user_id]
            for key in profile.keys():
                if key.lower() == "ник":
                    has_nick = True
                    break
        if has_nick:
            display_list.append(user_id)

    if not display_list:
        send_message(peer_id, "📋 В этой беседе нет участников с никами (кроме вас).")
        return

    display_list.sort(key=lambda uid: get_user_name_cached(uid, data).lower())

    lines = []
    for idx, uid in enumerate(display_list[:20], start=1):
        real_name = get_user_name_cached(uid, data)
        name_link = f"[id{uid}|{real_name}]"
        if peer_id in data.profiles and uid in data.profiles[peer_id]:
            profile = data.profiles[peer_id][uid]
            nick_value = None
            for key, value in profile.items():
                if key.lower() == "ник":
                    nick_value = value
                    break
            if nick_value:
                lines.append(f"{idx}. {name_link} — {nick_value}")
            else:
                lines.append(f"{idx}. {name_link} — без ника")
        else:
            lines.append(f"{idx}. {name_link} — без ника")

    text = "📋 Участники с никами:\n" + "\n".join(lines)
    if len(display_list) > 20:
        text += f"\n... и ещё {len(display_list)-20} человек."
    send_message(peer_id, text)

def handle_getnick(peer_id: int, from_id: int, args: List[str], data: BotData):
    if not args:
        send_message(peer_id, "❗ Укажите ник или часть ника для поиска: /getnick <ник>")
        return
    search = " ".join(args).lower()
    members = get_chat_members_cached(peer_id)
    if not members:
        send_message(peer_id, "Не удалось получить список участников.")
        return

    found = []
    for member in members:
        user_id = member.get("member_id")
        if not user_id or user_id < 0:
            continue
        if peer_id in data.profiles and user_id in data.profiles[peer_id]:
            profile = data.profiles[peer_id][user_id]
            for key, value in profile.items():
                if key.lower() == "ник" and value and search in value.lower():
                    found.append((user_id, value))
                    break

    if not found:
        send_message(peer_id, f"🔍 Пользователи с ником, содержащим '{search}', не найдены.")
        return

    lines = []
    for uid, nick in found:
        display = get_user_display(uid, data, peer_id)
        lines.append(f"{display} — {nick}")

    text = "🔍 Найдены пользователи:\n" + "\n".join(lines[:20])
    if len(found) > 20:
        text += f"\n... и ещё {len(found)-20} человек."
    send_message(peer_id, text)

def handle_clear(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if not is_bot_admin(peer_id):
        send_message(peer_id, "❌ Бот не является администратором беседы. Очистка невозможна.")
        return

    count = CLEAR_LIMIT
    if args and args[0].isdigit():
        count = min(int(args[0]), 100)

    messages = get_last_messages(peer_id, count)
    if not messages:
        send_message(peer_id, "📭 Сообщений для удаления не найдено.")
        return

    msg_ids = [msg["conversation_message_id"] for msg in messages]
    delete_messages(peer_id, msg_ids)
    data.add_log("очистил сообщения", from_id, from_id, peer_id, f"удалено {len(msg_ids)} сообщений")
    send_message(peer_id, f"✅ Удалено {len(msg_ids)} сообщений.")

def handle_clearallnicks(peer_id: int, from_id: int, args: List[str], data: BotData):
    if from_id not in data.admins:
        send_message(peer_id, "⛔ У вас нет прав на эту команду.")
        return
    if peer_id not in data.profiles:
        send_message(peer_id, "📭 В этой беседе нет анкет.")
        return
    count = 0
    for user_id in list(data.profiles[peer_id].keys()):
        profile = data.profiles[peer_id][user_id]
        if "Ник" in profile:
            del profile["Ник"]
            count += 1
            if not profile:
                del data.profiles[peer_id][user_id]
    if not data.profiles[peer_id]:
        del data.profiles[peer_id]
    data.add_log("очистил все ники", from_id, from_id, peer_id, f"удалено {count} ников")
    send_message(peer_id, f"✅ Удалено {count} ников у всех пользователей.")
    data.save()

def handle_help(peer_id: int, from_id: int, args: List[str], data: BotData):
    help_text = (
        "📖 Список команд:\n"
        "/start — активировать бота в беседе\n"
        "/setnick @user <ник> — установить ник.\n"
        "/rnick @user — удалить ник.\n"
        "/kick @user [причина: текст] — исключить пользователя.\n"
        "/allkick @user — исключить из всех бесед.\n"
        "/admin — список админов; /admin @user — добавить админа; /admin remove @user — снять админа\n"
        "/warn @user — выдать предупреждение.\n"
        "/unwarn @user — снять одно предупреждение.\n"
        "/allunwarn @user — снять все предупреждения.\n"
        "/warnlist — список пользователей с предупреждениями\n"
        "/online — участники онлайн\n"
        "/reg @user — дата добавления пользователя\n"
        "/regall — все участники с датами добавления\n"
        "/приветствие <текст> — установить приветствие (пустое — удалить).\n"
        "/шаблон поле1 | поле2 | ... — установить шаблон анкеты\n"
        "/форма значение1 | значение2 | ... — заполнить анкету\n"
        "/nonick — участники без ника\n"
        "/nlist — участники с никами\n"
        "/getnick <ник> — найти пользователей по нику\n"
        "/clear — удалить последние сообщения\n"
        "/clearallnicks — удалить ники у всех пользователей в беседе\n"
        "/help — показать это сообщение"
    )
    send_message(peer_id, help_text)

# ===== ФУНКЦИЯ ПРОВЕРКИ АКТИВНЫХ БЕСЕД =====
def clean_inactive_chats(data: BotData):
    for peer_id in list(data.active_chats):
        try:
            members = get_chat_members_cached(peer_id)
            bot_present = any(m.get("member_id") == -GROUP_ID for m in members)
            if not bot_present:
                print(f"Бот удалён из беседы {peer_id}, очищаем данные.")
                data.clear_chat_data(peer_id)
        except Exception as e:
            print(f"Ошибка проверки беседы {peer_id}: {e}")
            data.clear_chat_data(peer_id)

def clean_inactive_chats_by_time(data: BotData):
    now = time.time()
    to_remove = []
    for peer_id, last_ts in data.last_activity.items():
        if now - last_ts > INACTIVE_SECONDS:
            to_remove.append(peer_id)
    for peer_id in to_remove:
        print(f"Беседа {peer_id} неактивна более {INACTIVE_DAYS} дней, очищаем данные.")
        data.clear_chat_data(peer_id)

# ===== ОСНОВНОЙ ЦИКЛ =====
def main():
    print("Загрузка данных...")
    data = BotData()
    data.load()
    print("Данные загружены.")

    print("Проверка активных бесед...")
    clean_inactive_chats(data)
    clean_inactive_chats_by_time(data)

    print("Получение Long Poll сервера...")
    lp = get_long_poll_server()
    if not lp:
        return
    server = lp["server"]
    key = lp["key"]
    ts = lp["ts"]
    print(f"Подключено к {server}")

    commands = {
        "/start": handle_start,
        "/setnick": handle_setnick,
        "/rnick": handle_rnick,
        "/kick": handle_kick,
        "/allkick": handle_allkick,
        "/admin": handle_admin,
        "/warn": handle_warn,
        "/unwarn": handle_unwarn,
        "/allunwarn": handle_allunwarn,
        "/warnlist": handle_warnlist,
        "/online": handle_online,
        "/reg": handle_reg,
        "/regall": handle_regall,
        "/приветствие": handle_greeting,
        "/шаблон": handle_template,
        "/форма": handle_form,
        "/nonick": handle_nonick,
        "/nlist": handle_nlist,
        "/getnick": handle_getnick,
        "/clear": handle_clear,
        "/clearallnicks": handle_clearallnicks,
        "/help": handle_help,
    }

    last_cleanup_time = time.time()

    while True:
        try:
            url = f"{server}?act=a_check&key={key}&ts={ts}&wait=25"
            resp = requests.get(url, timeout=30)
            data_json = resp.json()

            if "failed" in data_json:
                if data_json["failed"] == 1:
                    ts = data_json["ts"]
                    continue
                elif data_json["failed"] in (2, 3):
                    lp = get_long_poll_server()
                    if lp:
                        server = lp["server"]
                        key = lp["key"]
                        ts = lp["ts"]
                    continue
                else:
                    time.sleep(5)
                    continue

            ts = data_json["ts"]

            # Очищаем старые записи из кеша дублей
            now = time.time()
            for key in list(processed_messages.keys()):
                if now - processed_messages[key] > DUPLICATE_CACHE_TTL:
                    del processed_messages[key]

            for update in data_json.get("updates", []):
                event_type = update.get("type")

                if event_type == "message_new":
                    msg = update.get("object", {}).get("message", {})
                    peer_id = msg.get("peer_id")
                    from_id = msg.get("from_id")
                    text = msg.get("text", "").strip()
                    action = msg.get("action")
                    conv_msg_id = msg.get("conversation_message_id")

                    # Защита от дублирования
                    if conv_msg_id:
                        cache_key = (peer_id, conv_msg_id)
                        if cache_key in processed_messages:
                            continue
                        processed_messages[cache_key] = now

                    if from_id == -GROUP_ID:
                        continue

                    if peer_id == from_id:
                        handle_private_message(peer_id, from_id, text, data)
                        continue

                    if peer_id in data.active_chats:
                        data.update_activity(peer_id)

                    if action and action.get("type") == "chat_invite_user":
                        inviter_id = from_id
                        invited_id = action.get("member_id")
                        if invited_id < 0:
                            continue
                        if invited_id == -GROUP_ID:
                            continue

                        if inviter_id == invited_id:
                            if peer_id in data.self_exited and invited_id in data.self_exited[peer_id]:
                                kick_from_chat(peer_id, invited_id)
                                data.add_log("кикнут (самовыход)", invited_id, from_id, peer_id, "попытка вернуться по ссылке")
                                continue
                        else:
                            if peer_id in data.self_exited and invited_id in data.self_exited[peer_id]:
                                data.self_exited[peer_id].remove(invited_id)
                                data.save()

                        if inviter_id not in data.admins and inviter_id != invited_id:
                            kick_from_chat(peer_id, invited_id)
                            data.add_log("исключён (не админ)", invited_id, inviter_id, peer_id, "попытка добавить без прав")
                            send_message(inviter_id, "У вас нет прав администратора для приглашения. Пользователь был исключён.")
                            continue

                        if peer_id not in data.join_dates:
                            data.join_dates[peer_id] = {}
                        data.join_dates[peer_id][invited_id] = int(time.time())
                        data.save()

                        if peer_id in data.greetings:
                            greeting_text = data.greetings[peer_id]
                            greeting_text = greeting_text.replace("{user}", get_user_display(invited_id, data, peer_id))
                            send_message(peer_id, greeting_text)

                        data.add_log("добавлен в беседу", invited_id, inviter_id, peer_id, "")
                        continue

                    if action and action.get("type") == "chat_kick_user":
                        kicked_id = action.get("member_id")
                        if kicked_id == -GROUP_ID:
                            print(f"Бот удалён из беседы {peer_id}, очищаем данные.")
                            data.clear_chat_data(peer_id)
                            members_cache.pop(peer_id, None)
                        else:
                            if from_id == kicked_id:
                                if peer_id not in data.self_exited:
                                    data.self_exited[peer_id] = set()
                                data.self_exited[peer_id].add(kicked_id)
                                data.save()
                            else:
                                if peer_id in data.self_exited and kicked_id in data.self_exited[peer_id]:
                                    data.self_exited[peer_id].remove(kicked_id)
                                    data.save()
                        continue

                    if peer_id not in data.active_chats:
                        if text.lower().startswith("/start"):
                            handle_start(peer_id, from_id, [], data)
                        continue

                    if from_id < 0:
                        continue

                    parts = text.split()
                    if not parts:
                        continue
                    cmd = parts[0].lower()
                    args = parts[1:] if len(parts) > 1 else []

                    if cmd in commands:
                        commands[cmd](peer_id, from_id, args, data)

            if time.time() - last_cleanup_time > CLEANUP_INTERVAL:
                clean_inactive_chats_by_time(data)
                last_cleanup_time = time.time()

        except Exception as e:
            print(f"Ошибка в цикле: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
