import websocket
import json
import ssl
import base64
import time
import threading
import subprocess
from datetime import datetime, timedelta, timezone
import requests
import sys
import os
from pathlib import Path
from flask import Flask, request, jsonify 

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def env_str(name, default=""):
    value = os.getenv(name)
    return default if value is None or value == "" else value

def parse_hhmm_to_minutes(value):
    hh, mm = str(value).strip().split(":", 1)
    return int(hh) * 60 + int(mm)

def parse_window(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    sep = "-" if "-" in raw else "~" if "~" in raw else None
    if not sep:
        return None
    start, end = [x.strip() for x in raw.split(sep, 1)]
    return parse_hhmm_to_minutes(start), parse_hhmm_to_minutes(end)

def is_minutes_in_window(minutes, start, end):
    if start == end:
        return False
    if start < end:
        return start <= minutes < end
    return minutes >= start or minutes < end

def is_window_active(window_value, now):
    win = parse_window(window_value)
    if not win:
        return False
    return is_minutes_in_window(now.hour * 60 + now.minute, win[0], win[1])

def is_primary_charge_window(now):
    return is_window_active(PRIMARY_CHARGE_WINDOW, now)

def is_solar_assist_charge_window(now):
    return is_window_active(SOLAR_ASSIST_CHARGE_WINDOW, now)

def is_configured_charge_window(now):
    return is_primary_charge_window(now) or is_solar_assist_charge_window(now)

def is_window_end_boundary(now):
    for raw in (PRIMARY_CHARGE_WINDOW, SOLAR_ASSIST_CHARGE_WINDOW):
        win = parse_window(raw)
        if not win:
            continue
        _, end = win
        if now.hour * 60 + now.minute == end:
            return True
    return False

def next_window_label(now):
    candidates = []
    cur = now.hour * 60 + now.minute
    for label, raw in [("primary_charge_window", PRIMARY_CHARGE_WINDOW), ("solar_assist_charge_window", SOLAR_ASSIST_CHARGE_WINDOW)]:
        win = parse_window(raw)
        if not win:
            continue
        start, end = win
        delta = (start - cur) % (24 * 60)
        candidates.append((delta, label, start, end))
    if not candidates:
        return "not configured"
    _, label, _, _ = sorted(candidates)[0]
    return label

# --- Discord Webhookの設定 ---
DISCORD_WEBHOOK_URL = env_str("DISCORD_WEBHOOK_URL", "")
CONFIG_FILE = env_str("POST_CONFIG_FILE", "config.json")

def send_discord_message(message_content):
    print(f"[{datetime.now(JST):%H:%M:%S}] [Discord送信] メッセージ: {message_content}")
    if not DISCORD_WEBHOOK_URL or "（" in DISCORD_WEBHOOK_URL: return
    try:
        requests.post(DISCORD_WEBHOOK_URL, data=json.dumps({"content": message_content}), headers={"Content-Type": "application/json"})
    except Exception as e: print(f"Discord送信失敗: {e}")

# --- WebSocket設定 ---
WS_URL = env_str("HEMS_WS_URL", "wss://localhost:8080/ws")
TARGET_DEVICE_CONTROLLER = env_str("TARGET_BATTERY_CONTROLLER", "")
EPC_BATTERY_LEVEL = "E4"
EPC_MODE = "DA"

# --- テスラ制御設定 ---
DEFAULT_TESLA_SCRIPT_DIR = str(Path(__file__).resolve().parent / "tesla_ops")
TESLA_DIR = env_str("TESLA_SCRIPT_DIR", DEFAULT_TESLA_SCRIPT_DIR)
TESLA_SCRIPT_PATH = env_str("TESLA_SET_AMPS_SCRIPT", f"{TESLA_DIR}/set_charge_amps.py")
TESLA_START_PATH = env_str("TESLA_START_SCRIPT", f"{TESLA_DIR}/start_charge.py")
TESLA_STOP_PATH = env_str("TESLA_STOP_SCRIPT", f"{TESLA_DIR}/stop_charge.py")
TESLA_FLEET_STATE_PATH = env_str("TESLA_FLEET_STATE_SCRIPT", f"{TESLA_DIR}/fleet_state_json.py")

# 制御モードの定義
MODE_AUTO = "auto"
MODE_CHARGE = "charge"
MODE_STANDBY = "standby"

# 制御ルール（時間帯の分割）
PRIMARY_CHARGE_WINDOW = env_str("PRIMARY_CHARGE_WINDOW", "")
SOLAR_ASSIST_CHARGE_WINDOW = env_str("SOLAR_ASSIST_CHARGE_WINDOW", "")
RESTART_AT = env_str("POST_RESTART_AT", "")

def should_restart_now(now, warn_only=False):
    raw = str(RESTART_AT or "").strip()
    if not raw:
        return False
    try:
        target = parse_hhmm_to_minutes(raw)
        current = now.hour * 60 + now.minute
        if warn_only:
            return current == (target - 5) % (24 * 60)
        return current == target
    except Exception:
        return False

# --- グローバル変数 ---
JST = timezone(timedelta(hours=+9), 'JST')
g_battery_state = {"level": -1, "mode": "UNKNOWN"}
g_intended_mode = "UNKNOWN"
status_lock = threading.RLock()
initial_state_ready = threading.Event()
ws_app = None
is_automatic_control_active = True
g_last_update_time = datetime.now(JST)

# 🌟 追加：テスラの充電状態を保持する変数
g_is_tesla_charging = False

app = Flask(__name__)

if not TARGET_DEVICE_CONTROLLER:
    print("WARNING: TARGET_BATTERY_CONTROLLER is not configured. Battery control will not work.")

# --- 設定の読み書き関数 ---
def load_config():
    if not os.path.exists(CONFIG_FILE):
        save_config({'night_threshold': 30, 'day_threshold': 30})
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
        
    if 'charge_threshold_percent' in config and 'night_threshold' not in config:
        config['night_threshold'] = config['charge_threshold_percent']
        config['day_threshold'] = 30
        save_config(config)
    return config

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

config = load_config()
NIGHT_THRESHOLD = config.get('night_threshold', 30)
NIGHT_THRESHOLD_LOWER = max(0, NIGHT_THRESHOLD - 2)
DAY_THRESHOLD = config.get('day_threshold', 30)
DAY_THRESHOLD_LOWER = max(0, DAY_THRESHOLD - 2)

# --- 🚗 テスラ操作（非同期） ---
def set_tesla_amps(amps, source="Auto"):
    def task():
        now = datetime.now(JST)
        msg = f"🚗 [テスラ制御] {source}からの要求で {amps}A に変更を開始します。"
        print(f"[{now:%H:%M:%S}] {msg}")
        send_discord_message(msg)
        
        try:
            result = subprocess.run(["python3", TESLA_SCRIPT_PATH, str(amps)], capture_output=True, text=True)
            try:
                res_data = json.loads(result.stdout)
                if res_data.get("status") == "success": send_discord_message(f"✅ [テスラ制御成功] アンペアを {amps}A に設定しました。")
                else: send_discord_message(f"❌ [テスラ制御エラー] 失敗: {res_data.get('msg', '不明なエラー')}")
            except json.JSONDecodeError: send_discord_message(f"⚠️ [テスラ制御] 実行完了 (解析失敗): {result.stdout}")
        except Exception as e: send_discord_message(f"🚨 [テスラ制御致命的エラー] 実行失敗: {e}")
    threading.Thread(target=task, daemon=True).start()

# --- 蓄電池 即時制御（イベント駆動）関数 ---
def check_and_apply_mode_immediately():
    global is_automatic_control_active, g_intended_mode, g_is_tesla_charging
    if not is_automatic_control_active: return

    now = datetime.now(JST)
    with status_lock:
        current_level = g_battery_state["level"]
        current_mode = g_battery_state["mode"]
    
    if current_level == -1: return
    new_intended_mode = g_intended_mode

    # Configured primary window switching logic
    if is_primary_charge_window(now):
        if g_is_tesla_charging:
            # 🚗 テスラ充電中：蓄電池からの放電を完全にブロック
            if current_level <= NIGHT_THRESHOLD_LOWER:
                new_intended_mode = MODE_CHARGE
            else:
                if current_mode != MODE_STANDBY and current_mode != MODE_CHARGE:
                    new_intended_mode = MODE_STANDBY
        else:
            # 🚗 テスラ停止中：通常ロジック（完了したらAUTOにして家の負荷を賄う）
            if current_level <= NIGHT_THRESHOLD_LOWER:
                new_intended_mode = MODE_CHARGE
            elif current_level >= NIGHT_THRESHOLD:
                if current_mode == MODE_CHARGE or current_mode == MODE_STANDBY:
                    new_intended_mode = MODE_AUTO
                    
    elif is_solar_assist_charge_window(now):
        if current_mode != MODE_STANDBY and current_level >= DAY_THRESHOLD:
            new_intended_mode = MODE_STANDBY
        elif current_mode == MODE_STANDBY and current_level <= DAY_THRESHOLD_LOWER:
            new_intended_mode = MODE_AUTO
            
    elif is_window_end_boundary(now) and g_intended_mode != MODE_AUTO:
        new_intended_mode = MODE_AUTO

    if new_intended_mode != g_intended_mode:
        print(f"[{now:%H:%M:%S}] ⚡ [即時制御] 目標到達！モードを '{new_intended_mode}' に変更します。")
        send_discord_message(f"⚡ [即時制御] 目標到達！ {current_mode} ➔ {new_intended_mode} に変更します。")
        set_operation_mode(new_intended_mode, source="即時イベント")

# --- WebSocketコールバック関数 ---
def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("type")
    if msg_type == "initial_state":
        update_state_from_payload(data.get("payload", {}))
        initial_state_ready.set()
    elif msg_type == "property_changed":
        update_state_from_payload(data.get("payload", {}))

def update_state_from_payload(payload):
    device_data = payload.get("devices", {}).get(TARGET_DEVICE_CONTROLLER)
    if not device_data:
         if payload.get('eoj') and f"{payload.get('ip')} {payload.get('eoj')}" == TARGET_DEVICE_CONTROLLER:
             update_state_from_property_change(payload)
         return
    with status_lock:
        if "properties" in device_data: update_state_from_properties(device_data["properties"])

def update_state_from_properties(properties):
    global g_battery_state, g_last_update_time
    updated = False
    if EPC_BATTERY_LEVEL in properties:
        prop = properties[EPC_BATTERY_LEVEL]
        new_level = g_battery_state["level"]
        if "number" in prop: new_level = prop["number"]
        elif "EDT" in prop:
            try: new_level = int.from_bytes(base64.b64decode(prop["EDT"]), byteorder='big')
            except: pass
        if g_battery_state["level"] != new_level:
            g_battery_state["level"] = new_level; updated = True
            
    if EPC_MODE in properties:
        prop = properties[EPC_MODE]
        if "string" in prop and g_battery_state["mode"] != prop["string"]:
            g_battery_state["mode"] = prop["string"]; updated = True
            
    if updated:
        g_last_update_time = datetime.now(JST)
        print(f"[{datetime.now(JST):%H:%M:%S}] [状態更新] 残量: {g_battery_state['level']}% | モード: {g_battery_state['mode']}")
        check_and_apply_mode_immediately()

def update_state_from_property_change(payload):
    with status_lock: update_state_from_properties({payload.get('epc'): payload.get('value', {})})

def on_error(ws, error):
    if not isinstance(error, KeyboardInterrupt): print(f"WebSocketエラー: {error}")

def on_close(ws, code, msg): initial_state_ready.clear()
def on_open(ws): print("WebSocket接続が開かれました。")
    
def set_operation_mode(mode_name, source="auto"):
    global g_intended_mode
    if not (ws_app and ws_app.sock and ws_app.sock.connected): return
    with status_lock: g_intended_mode = mode_name.lower()
    payload = {
        "type": "set_properties", "payload": { "target": TARGET_DEVICE_CONTROLLER, "properties": { EPC_MODE: { "string": mode_name.lower() } } },
        "requestId": f"set_mode_{mode_name}_{time.time()}"
    }
    ws_app.send(json.dumps(payload))
    print(f"[{datetime.now(JST):%H:%M:%S}] [コマンド送信] モードを'{mode_name}'に変更 ({source})")
    
def request_status_update():
    if not (ws_app and ws_app.sock and ws_app.sock.connected): return
    ws_app.send(json.dumps({"type": "update_properties", "payload": {"targets": [TARGET_DEVICE_CONTROLLER], "force": True}, "requestId": f"update_req_{time.time()}"}))

def auto_control_loop():
    global g_intended_mode, g_last_update_time, g_is_tesla_charging
    initial_state_ready.wait(timeout=120)
    
    now = datetime.now(JST)
    minutes_to_wait = 5 - (now.minute % 5)
    seconds_to_sleep = (minutes_to_wait * 60 - now.second) % 300
    if seconds_to_sleep > 0: time.sleep(seconds_to_sleep)

    with status_lock:
        g_intended_mode = g_battery_state["mode"]
        send_discord_message(f"✅ 蓄電池制御クライアントを開始しました。残量: {g_battery_state['level']}%, モード: {g_battery_state['mode']}")

    while True:
        try:
            now = datetime.now(JST)
            remaining_seconds = (300 - (now.minute * 60 + now.second) % 300) % 300
            if remaining_seconds > 0: time.sleep(remaining_seconds)

            current_time = datetime.now(JST)
            request_status_update()
            time.sleep(10)

            if should_restart_now(current_time, warn_only=True):
                send_discord_message("🔄 定期的な再起動をまもなく実施します。")

            next_event_desc = next_window_label(current_time)
            
            with status_lock:
                current_level = g_battery_state["level"]
                current_mode = g_battery_state["mode"]
            
            send_discord_message(f"📊 定期チェック: 残量 {current_level}% | モード {current_mode} | 次の予定: {next_event_desc}")

            if is_automatic_control_active:
                now = datetime.now(JST)
                new_intended_mode = g_intended_mode

                # Loop-side configured primary window switching logic
                if is_primary_charge_window(now):
                    if current_level != -1:
                        if g_is_tesla_charging:
                            if current_level <= NIGHT_THRESHOLD_LOWER:
                                new_intended_mode = MODE_CHARGE
                            else:
                                if current_mode != MODE_STANDBY and current_mode != MODE_CHARGE:
                                    new_intended_mode = MODE_STANDBY
                        else:
                            if current_level <= NIGHT_THRESHOLD_LOWER:
                                new_intended_mode = MODE_CHARGE
                            elif current_level >= NIGHT_THRESHOLD:
                                if current_mode == MODE_CHARGE or current_mode == MODE_STANDBY:
                                    new_intended_mode = MODE_AUTO
                
                elif is_solar_assist_charge_window(now):
                    if current_level != -1:
                        if current_mode != MODE_STANDBY and current_level >= DAY_THRESHOLD:
                            new_intended_mode = MODE_STANDBY
                        elif current_mode == MODE_STANDBY and current_level <= DAY_THRESHOLD_LOWER:
                            new_intended_mode = MODE_AUTO
                
                elif is_window_end_boundary(now) and g_intended_mode != MODE_AUTO:
                    new_intended_mode = MODE_AUTO
                
                if new_intended_mode != g_intended_mode:
                    set_operation_mode(new_intended_mode, source="定期フェールセーフ")
            
        except Exception as e: print(f"制御ループエラー: {e}"); time.sleep(60)

# 🌟 コマンド処理ロジック (CLI/API 共通)
def process_command(command, value=None, source="CLI"):
    global is_automatic_control_active, g_intended_mode, NIGHT_THRESHOLD, NIGHT_THRESHOLD_LOWER, DAY_THRESHOLD, DAY_THRESHOLD_LOWER
    command = command.lower()
    
    # 🌟 追加：READ VM からテスラの充電状態を受け取る
    if command == 'update_tesla_state':
        global g_is_tesla_charging
        is_charging = bool(value)
        if g_is_tesla_charging != is_charging:
            g_is_tesla_charging = is_charging
            state_str = "開始" if is_charging else "停止"
            send_discord_message(f"🚗 [情報共有] テスラの充電『{state_str}』を検知しました。蓄電池の防衛ロジックを再評価します。")
            check_and_apply_mode_immediately()
        return True, "テスラ状態を同期しました。"

    # --- 🚗 テスラ充電の開始・停止 ---
    elif command == 'tesla_start':
        def task_start():
            send_discord_message(f"🚗 [テスラ制御] {source}からの要求で 充電開始 を実行します。")
            try:
                result = subprocess.run(["python3", TESLA_START_PATH], capture_output=True, text=True)
                try:
                    res_data = json.loads(result.stdout)
                    if res_data.get("status") == "success": send_discord_message("✅ [テスラ制御成功] 充電を開始しました。")
                    else: send_discord_message(f"❌ [テスラ制御エラー] 失敗: {res_data.get('msg', '不明なエラー')}")
                except: send_discord_message(f"⚠️ [テスラ制御] 実行完了 (解析失敗): {result.stdout}")
            except Exception as e: send_discord_message(f"🚨 [テスラ制御致命的エラー] 実行失敗: {e}")
        threading.Thread(target=task_start, daemon=True).start()
        return True, "テスラ充電開始を受け付けました。"

    elif command == 'tesla_stop':
        def task_stop():
            send_discord_message(f"🚗 [テスラ制御] {source}からの要求で 充電停止 を実行します。")
            try:
                result = subprocess.run(["python3", TESLA_STOP_PATH], capture_output=True, text=True)
                try:
                    res_data = json.loads(result.stdout)
                    if res_data.get("status") == "success": send_discord_message("✅ [テスラ制御成功] 充電を停止しました。")
                    else: send_discord_message(f"❌ [テスラ制御エラー] 失敗: {res_data.get('msg', '不明なエラー')}")
                except: send_discord_message(f"⚠️ [テスラ制御] 実行完了 (解析失敗): {result.stdout}")
            except Exception as e: send_discord_message(f"🚨 [テスラ制御致命的エラー] 実行失敗: {e}")
        threading.Thread(target=task_stop, daemon=True).start()
        return True, "テスラ充電停止を受け付けました。"

    # --- 🚗 テスラアンペア設定 ---
    elif command == 'tesla':
        if value is not None:
            try:
                amps = int(value)
                set_tesla_amps(amps, source=source)
                return True, f"テスラアンペア変更({amps}A)を受け付けました。"
            except: return False, "アンペアは数値で入力してください。"
        return False, "アンペア数を指定してください。(例: tesla 10)"

    # --- 🔋 蓄電池モード/閾値制御 ---
    elif command == 'auto_on':
        is_automatic_control_active = True
        with status_lock: g_intended_mode = g_battery_state["mode"]
        return True, "自動制御を有効にしました。"
    elif command == 'auto_off':
        is_automatic_control_active = False
        return True, "自動制御を無効にしました。"
    elif command in [MODE_AUTO, MODE_CHARGE, MODE_STANDBY]:
        is_automatic_control_active = False
        set_operation_mode(command, source=source)
        return True, f"モードを {command} に変更しました。"
        
    elif command in ['thre_night', 'thre_day', 'thre_primary', 'thre_solar_assist']:
        if value is not None:
            try:
                new_threshold = int(value)
                if 0 <= new_threshold <= 100:
                    conf = load_config()
                    target_name = "primary_charge_window" if command in ['thre_night', 'thre_primary'] else "solar_assist_charge_window"
                    if command in ['thre_night', 'thre_primary']:
                        NIGHT_THRESHOLD = new_threshold
                        NIGHT_THRESHOLD_LOWER = max(0, new_threshold - 2)
                        conf['night_threshold'] = NIGHT_THRESHOLD
                    else:
                        DAY_THRESHOLD = new_threshold
                        DAY_THRESHOLD_LOWER = max(0, new_threshold - 2)
                        conf['day_threshold'] = DAY_THRESHOLD
                    save_config(conf)
                    send_discord_message(f"⚙️ {target_name}の目標SOCを {new_threshold}% に変更しました。")
                    return True, f"{target_name}目標を {new_threshold}% に設定しました。"
                return False, "0から100の間で入力してください。"
            except: return False, "数値を入力してください。"
        return True, f"現在の設定 - primary_charge_window: {NIGHT_THRESHOLD}%, solar_assist_charge_window: {DAY_THRESHOLD}%"
            
    elif command == 'restart':
        if ws_app: ws_app.close()
        os.execv(sys.executable, ['python3'] + sys.argv)
    
    return False, "不正なコマンドです。"


@app.route('/api/tesla/fleet_state', methods=['GET'])
def api_tesla_fleet_state():
    """READ側から必要時だけ呼ばれるFleet API代理取得。制御判断はしない。"""
    try:
        result = subprocess.run(
            ["python3", TESLA_FLEET_STATE_PATH],
            cwd=TESLA_DIR,
            capture_output=True,
            text=True,
            timeout=45,
        )
        if result.returncode != 0:
            return jsonify({
                "status": "error",
                "stage": "subprocess",
                "returncode": result.returncode,
                "stdout": result.stdout[-1000:],
                "stderr": result.stderr[-1000:],
            }), 502
        try:
            data = json.loads(result.stdout)
        except Exception as e:
            return jsonify({
                "status": "error",
                "stage": "json_parse",
                "message": str(e),
                "stdout": result.stdout[-1000:],
                "stderr": result.stderr[-1000:],
            }), 502
        return jsonify(data), 200 if data.get("status") == "success" else 502
    except subprocess.TimeoutExpired:
        return jsonify({"status": "error", "stage": "timeout"}), 504
    except Exception as e:
        return jsonify({"status": "error", "stage": "exception", "message": str(e)}), 500

@app.route('/api/command', methods=['POST'])
def api_command():
    data = request.json
    success, message = process_command(data.get('action'), data.get('value'), source="API")
    return jsonify({"status": "success" if success else "error", "message": message})

@app.route('/api/status', methods=['GET'])
def api_status():
    with status_lock:
        return jsonify({
            "battery_level": g_battery_state["level"],
            "mode": g_battery_state["mode"],
            "intended_mode": g_intended_mode,
            "auto_active": is_automatic_control_active,
            "night_threshold": NIGHT_THRESHOLD,
            "day_threshold": DAY_THRESHOLD,
            "last_update": g_last_update_time.strftime('%Y-%m-%d %H:%M:%S')
        })

def interactive_cli_loop():
    initial_state_ready.wait(timeout=120)
    print("\n--- 手動操作モード ---")
    print("コマンド: charge | auto | standby | auto_on | auto_off")
    print("       thre_primary [0-100] | thre_solar_assist [0-100]") 
    print("       tesla_start | tesla_stop | tesla [amps] | exit | restart")
    while True:
        try:
            user_input = input(" > ").strip().lower().split()
            if not user_input: continue
            if user_input[0] == 'exit': break
            success, msg = process_command(user_input[0], user_input[1] if len(user_input) > 1 else None, source="CLI")
            print(msg)
        except Exception as e: print(f"エラー: {e}")

if __name__ == "__main__":
    now = datetime.now(JST)
    if should_restart_now(now):
        os.execv(sys.executable, ['python3'] + sys.argv)

    ws_app = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    threading.Thread(target=lambda: ws_app.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}), daemon=True).start()
    threading.Thread(target=auto_control_loop, daemon=True).start()
    threading.Thread(target=lambda: app.run(host=env_str('POST_FLASK_HOST', '0.0.0.0'), port=int(env_str('POST_FLASK_PORT', '5000')), use_reloader=False), daemon=True).start()
    
    try: interactive_cli_loop()
    except KeyboardInterrupt: pass
    finally:
        if ws_app: ws_app.close()
        sys.exit(0)