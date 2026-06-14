import websocket
import json
import ssl
import base64
import threading
import time
import requests
import pandas as pd
import pvlib
from pvlib.location import Location
import paho.mqtt.client as mqtt
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timezone, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import math
import os
import traceback

# ==========================================
# --- Public-safe environment configuration ---
# ==========================================
# .env is optional. In production, systemd/Docker environment variables are recommended.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def env_str(name, default=""):
    value = os.getenv(name)
    return default if value is None or value == "" else value

def env_int(name, default=0):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)

def env_float(name, default=0.0):
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)

def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None or value == "":
        return bool(default)
    return str(value).strip().lower() in ("1", "true", "yes", "on", "y")

def env_json_list(name, default=None):
    default = default or []
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = json.loads(raw)
        if isinstance(value, list):
            return value
    except Exception:
        pass
    return [item.strip() for item in raw.split(",") if item.strip()]

POST_CONTROL_BASE_URL = env_str("POST_CONTROL_BASE_URL", "http://127.0.0.1:5000")

def post_control_url(path):
    return POST_CONTROL_BASE_URL.rstrip("/") + path


# Smart HEMS Controller
# Public-safe edition: runtime-specific values are loaded from environment variables.
# Do not commit .env, tokens, logs, or local JSON caches.
# Public-safe edition: contract-specific charge windows are configured locally.
# ==========================================
# --- Configuration & Settings Management ---
# ==========================================
WS_URL = env_str("HEMS_WS_URL", "wss://localhost:8080/ws")
MQTT_HOST = env_str("MQTT_HOST", "127.0.0.1")
JST = timezone(timedelta(hours=9))

LAT = env_float("HOME_LATITUDE", 0.0)
LON = env_float("HOME_LONGITUDE", 0.0)

TARGET_SOLAR = env_str("TARGET_SOLAR", "")
TARGET_BATTERY = env_str("TARGET_BATTERY", "")

DEFAULT_SETTINGS = {
    "SYSTEM_CAPACITY_W": 2400,
    "SYSTEM_LOSS_FACTOR": 0.822,
    "BATT_MAX_WH": 12600,
    "GRID_LIMIT_W": 6400,
    "WARNING_LIMIT_W": 6200,
    "CRITICAL_LIMIT_W": 6350,
    "TARGET_SAFE_POWER_W": 6000,
    "MAX_CHARGE_AMPS": 32,
    "TESLA_HEAVY_CHARGE_KM": 30.0, 
    "DISCORD_WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL", "")
}
SETTINGS_FILE = env_str("SMART_HEMS_SETTINGS_FILE", "smart_hems_settings.json")

sys_settings = DEFAULT_SETTINGS.copy()

def load_sys_settings():
    global sys_settings
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if v == "": continue
                    if k == "DISCORD_WEBHOOK_URL":
                        sys_settings[k] = str(v)
                    else:
                        try:
                            sys_settings[k] = float(v) if '.' in str(v) else int(v)
                        except ValueError:
                            sys_settings[k] = v
        except Exception as e:
            print(f"🚨 設定ファイルの読み込みエラー: {e}")

def save_sys_settings():
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(sys_settings, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"🚨 設定ファイルの保存エラー: {e}")

load_sys_settings()

# グローバル状態管理変数
g_warning_start_time = None
g_last_decrease_time = None   
g_last_increase_time = None   
g_last_tesla_target_amps = None
g_last_tesla_target_since = None
g_last_tesla_amp_command_time = None
g_last_tesla_commanded_amps = None

g_low_surplus_start_time = None 
g_last_notified_tesla_charging = None 
g_last_tesla_start_time = None 
g_last_skip_log_time = None
g_last_reset_hour = -1
g_tesla_start_retries = 0
g_tesla_suspended = False
g_read_only_mode = False
g_last_tesla_amp_suppress_log_time = None
g_last_tesla_idle_reset_log_time = None
g_last_tesla_critical_drop_time = None

# ☀️ solar assist制御用フラグ
g_last_day_standby_time = None
g_day_standby_by_auto = False

# solar assist windowの優先順位ラッチ。
# Teslaと蓄電池を同時に最適化せず、まずどちらを先に満たすかを固定する。
g_day_priority_state = "none"   # none / battery_first / tesla_first
g_day_priority_since = None
g_day_priority_reason = ""
g_day_priority_target_soc = 0
g_day_priority_last_switch = None

# Tesla A変更 / Battery First ログ抑制用グローバル
g_last_tesla_commanded_amps = None
g_last_tesla_commanded_time = None
# Battery First維持ログ/再送抑制
g_last_battery_first_log_time = None
g_last_battery_first_log_key = ""


# 手動Day Target <span class="help-btn" onclick="showHelp('ai_targets')">?</span>はconfigured hold windowまでAI上書きを抑制する
g_manual_day_target_until = None
g_manual_day_target_value = None

g_ctrl_status = {"status": "auto", "night_threshold": 30, "day_threshold": 30}

# 蓄電池コマンド重複送信防止
# /api/status が mode/intended_mode 形式で返る場合でも、READ側で同一モードを再送し続けないようにする。
g_last_battery_command_action = None
g_last_battery_command_time = None

# 蓄電池コマンド最終調停ログ抑制
g_last_battery_policy_block_time = None
g_last_battery_policy_block_key = ""
g_battery_command_min_interval_sec = 45

# 制御ループ多重起動防止。WebSocket更新と5秒周期workerが同時に入るのを防ぐ。
g_control_lock = threading.Lock()
g_last_control_skip_log_time = None


app = Flask(__name__)
CORS(app)

# --- 状態データ ---
state = {
    "solar": 0, "grid": 0, "battery": 0, "soc": 0, "rem_wh": 0, "last_update": ""
}

tesla_state = {
    "soc": 0, "range": 0, "charging_state": "Stopped", "power": 0.0,
    "charger_power": 0.0,
    "time_to_full": 0.0, "actual_current": 0, "request_current": 0,
    "charge_limit": 100, "is_custom": False, "plugged_in": False, "max_range": 397.0,
    # TeslaMate MQTT geofence/location based home control gate.
    # home_charging=True になった時だけHEMSはTeslaへstart/stop/ampsを送る。
    "home_charging": False,
    "home_charge_reason": "unknown",
    "geofence": "",
    "is_home": None,
    "location": None,
    "control_allowed": False,
    "home_distance_m": None
}

# Wall Connector実測ベースのEV負荷状態。
# Fleet APIはこの負荷が立ち上がった瞬間だけPOST側へ問い合わせ、分類結果を保持する。
ev_wall_state = {
    "enabled": True,
    "api_ok": False,
    "vehicle_connected": False,
    "contactor_closed": False,
    "current_a": 0.0,
    "grid_v": 0.0,
    "power_w": 0.0,
    "session_energy_wh": 0.0,
    "evse_state": None,
    "load_active": False,
    "load_type": "none",  # none / charging / climate_only / charging_with_climate / unknown_ev_load
    "load_since": None,
    "last_power_time": None,
    "last_activity_time": None,
    "low_power_since": None,
    "last_update": None,
    "last_error": "",
    "fleet_checked_for_session": False,
    "fleet_last_fetch": None,
    "fleet_disabled_until": None,
    "fleet_data": None,
}

# Tesla SOC/rangeのlast-known cache。
# Wall ConnectorにはSOC/km情報が無いため、Fleet分類時に得たbattery_levelを保存し、
# 純粋なcharging中だけWall Connectorのsession_energy_wh増加分で推定加算する。
g_tesla_range_cache = {
    "soc_percent": None,
    "range_km": None,
    "max_range_km": 397.0,
    "charge_limit_percent": 100.0,
    "target_range_km": 397.0,
    "missing_km": None,
    "source": "unknown",
    "last_update": None,
    "is_estimated": True,
    "fleet_classification": None,
    "wall_estimate_active": False,
    "wall_estimate_started_at": None,
    "wall_estimate_start_session_wh": None,
    "wall_estimate_base_range_km": None,
    "wall_estimate_base_soc_percent": None,
}

# Fleet API取得制御。
# 通常時はWall Connectorイベント駆動、昼の計画時間帯だけ5分ポーリング。
# TeslaMate MQTT取得コードは残すが、デフォルトでは呼び出さない。
g_last_fleet_planning_poll_time = None
g_last_fleet_planning_poll_reason = ""

forecast_data = {
    "gen_afternoon_kwh": 0, "predicted_soc_window_end": 50, "ai_needs_grid": False, 
    "last_calc": None, "today_date": "--/--", "tomorrow_date": "--/--",
    "today_hourly": [], "tomorrow_hourly": [], "today_total": 0, "tomorrow_total": 0,
    "tesla_needs_heavy_charge": False
}

settings = {
    "day_auto_mode": True,
    "tesla_auto_mode": True,
    "tesla_min_soc": 50,
    # Tesla充電方針: auto / solar_priority / manual
    "tesla_charge_mode": "auto",
    # Contract-specific charging windows.
    # Keep concrete times in local .env / smart_hems_settings.json, not in the public repo.
    # Format: HH:MM-HH:MM. Empty value disables that window.
    "primary_charge_window": env_str("PRIMARY_CHARGE_WINDOW", ""),
    "solar_assist_charge_window": env_str("SOLAR_ASSIST_CHARGE_WINDOW", ""),
    "solar_planning_poll_window": env_str("SOLAR_PLANNING_POLL_WINDOW", ""),
    "debug_mode": False,
    # solar assist windowのSolar Priorityで許容する少量買電
    "solar_priority_grid_assist_w": 200,
    "solar_priority_min_amps": 5,
    # 夕方までに確保したいSOC目標
    "afternoon_target_soc": 70,
    "afternoon_target_hour": 17,
    "afternoon_recovery_mode": True,
    # 前日夜に低発電日の運用を提案するモード
    "low_solar_proposal_mode": True,
    "low_solar_threshold_kwh": 5.0,
    "storm_solar_threshold_kwh": 3.0,
    "proposal_low_night_target": 50,
    "proposal_storm_night_target": 60,
    "proposal_low_day_target": 100,
    "proposal_tesla_small_km": 10,
    "proposal_dismissed_date": "",
    # Night Targetのチャタリング防止幅。例: target 30%なら33%まで充電してからStandby保持。
    "night_target_hysteresis_percent": 3,
    # Teslaが自宅充電かを判定するための安全設定
    # Tesla制御は「自宅にいる」と確定できた時だけ許可する。
    # 電力一致だけで自宅判定すると、外部6kW普通充電を誤判定する可能性があるため、
    # デフォルトでは meter_only 判定を制御許可に使わない。
    "tesla_control_only_when_home_charging": True,
    "tesla_home_detection_mode": "strict",  # strict / meter_fallback
    "tesla_allow_meter_only_home_detection": False,
    "tesla_home_charging_min_power_w": 300,
    "tesla_home_charging_max_power_w": 12000,
    "tesla_home_charging_meter_margin_w": 1200,
    "tesla_home_geofence_names": env_json_list("TESLA_HOME_GEOFENCE_NAMES", ["home", "自宅", "house"]),
    "tesla_home_latitude": env_float("TESLA_HOME_LATITUDE", LAT),
    "tesla_home_longitude": env_float("TESLA_HOME_LONGITUDE", LON),
    "tesla_home_radius_m": 120,

    "tesla_amp_increase_cooldown_sec": 120,
    "tesla_amp_decrease_cooldown_sec": 60,
    "tesla_amp_change_deadband_a": 2,
    "tesla_amp_target_hold_sec": 30,
    "battery_soc_decision_deadband_percent": 2,

    "tesla_amp_cheap_increase_cooldown_sec": 45,
    "tesla_amp_cheap_decrease_cooldown_sec": 45,

    # 安価充電時間帯はTesla充電量を最大化する。
    # 旧来の TARGET_SAFE_POWER_W=6000W は「安全寄り」だったため、
    # configured contract charge windows は GRID_LIMIT_W=6400W 直下を狙う。
    # headroomを100W取るため、実目標は概ね6300W。超過時はWall Connector実測Aを基準に下げる。
    "tesla_grid_maximize_enabled": True,
    "tesla_grid_max_only_cheap_window": True,
    "tesla_grid_max_target_w": 6400,
    "tesla_grid_max_headroom_w": 100,
    "tesla_grid_max_warning_margin_w": 50,
    "tesla_grid_max_critical_over_w": 0,
    "tesla_grid_max_increase_margin_w": 250,
    "tesla_grid_max_max_step_a": 3,
    "tesla_grid_max_feedback_enabled": True,
    # 6.4kWを超えた状態が続く場合は、通常のA変更クールダウンを待たずに段階降下を繰り返す。
    # 32A開始直後など、1回の降下だけでは買電がまだ上限超過するケースのブレーカー保護用。
    "tesla_breaker_critical_retry_sec": 5,
    "tesla_breaker_warning_retry_sec": 15,
    "tesla_breaker_force_min_drop_a": 1,
    "tesla_breaker_commanded_floor_sec": 120,

    "tesla_amp_large_diff_a": 4,
    "tesla_amp_large_diff_cooldown_sec": 30,
    "tesla_amp_very_large_diff_a": 8,
    "tesla_amp_very_large_diff_cooldown_sec": 15,

    "day_priority_mode": "auto",  # auto / battery_first / tesla_first
    "day_priority_hold_sec": 300,
    "day_priority_battery_first_day_target_threshold": 90,
    "day_priority_tesla_first_missing_km": 30,
    "day_priority_release_margin_percent": 1,
    "battery_first_min_tesla_amps": 5,
    "battery_first_reserve_min_w": 1800,
    "battery_first_reserve_max_w": 3600,

    # solar-assist window early phaseは蓄電池側が自己診断で動かないため、Teslaを優先する。
    # Tesla自体は200V 32A=約6.4kWまで許可する。
    # ただし実買電が危険域に入った場合は、既存のブレーカー保護が最優先で下げる。
    "day_early_tesla_priority_until_minute": 10,
    "day_early_tesla_target_grid_w": 6400,
    # solar-assist window early phaseでも、小不足かつ太陽光だけで回復できる時は買電Boostしない。
    "day_early_respect_solar_recovery": True,

    # Teslaが先に自宅充電中で、蓄電池目標がsolar window endまでにまだ間に合うなら、Tesla完了まで優先する。
    "day_tesla_hold_until_charge_done": True,
    "day_tesla_finish_grace_min": 10,
    "day_battery_feasible_efficiency": 0.90,

    # solar assist windowは「瞬間電力の均等配分」ではなく、solar window endまでの残り時間で
    # Tesla / 蓄電池をどちらから太く充電するかを決める。
    "day_energy_scheduler_enabled": True,
    # Teslaを6kW近くで充電した時の回復量。実測ベース。
    "tesla_full_power_km_per_hour": 46.0,
    # 200V 5A程度で太陽光のみを流した時の回復量。実測レンジの控えめ側。
    "tesla_min_amp_km_per_hour": 6.0,
    # 蓄電池5kW入力時のSOC回復量。12.7kWhの約40%/h。
    "battery_full_charge_soc_per_hour": 40.0,
    # Tesla優先を許可する最低蓄電池SOC。これ未満なら蓄電池を守る。
    "day_tesla_priority_min_battery_soc": 40,
    # スケジューリング上の安全余白。予測外の負荷・雲・API遅延対策。
    "day_time_split_margin_min": 10,
    # Tesla不足がこの値以下で、太陽光5A継続でsolar window endまでに戻せるなら買電Boostしない。
    "solar_recovery_max_missing_km": 15,
    "solar_recovery_min_surplus_w": 700,
    "solar_recovery_grid_assist_w": 100,
    # この距離未満ではTesla startを積極送信しない。Tesla側が開始拒否しやすいため。
    "tesla_start_min_missing_km": 8,
    # Teslaが充電開始を受け付けやすい最大航続距離目安。
    # 経験則では388km以下。バッテリー劣化や車両挙動変化に合わせて調整する。
    # 0以下にするとこの判定を無効化する。
    "tesla_start_max_range_km": 388.0,

    # Teslaが充電完了/停止中でも、充電器経由で空調・温調・補機電力を使うことがある。
    # 安価充電時間帯外では、32Aなどの残留A設定を安全側の5Aへ戻す。
    # ただし configured contract charge windows はこれから充電開始する時間帯なのでリセットしない。
    "tesla_aux_power_detect_kw": 0.3,
    "tesla_idle_safe_amps": 5,
    "tesla_idle_safe_amps_reset_enabled": True,

    # この距離以上なら、蓄電池が間に合う範囲でTesla Boost候補。
    "day_boost_grid_threshold_km": 20,

    # Wall Connector local API + Fleet API event classification.
    # READ側が監視・判断し、POST側はFleet API代理取得と実行だけを担当する。
    "wall_connector_enabled": True,
    "wall_connector_ip": env_str("WALL_CONNECTOR_IP", ""),
    "wall_poll_interval_sec": 5,
    # Wall Connectorは自宅固定IPのため、接続/実電力をTesla自宅制御の主センサーとして使う。
    "tesla_control_allow_wallconnector_home": True,
    "wall_connector_current_as_control_source": True,
    "wall_connector_power_as_control_source": True,
    "wall_power_on_w": 200,
    "wall_power_off_w": 100,
    # 低出力の空調/温調維持をEV負荷として追跡する下限。
    # POST側へ「充電中」とは通知しないが、Fleet再分類の対象にする。
    "wall_activity_min_w": 50,
    "wall_activity_min_a": 0.2,
    "ev_session_end_sec": 180,
    "fleet_state_endpoint": env_str("FLEET_STATE_ENDPOINT", post_control_url("/api/tesla/fleet_state")),
    "fleet_classify_ttl_sec": 900,
    "fleet_error_backoff_sec": 1800,
    # Fleet APIの計画用取得。SOC/kmのための常時ポーリングはしない。
    # solar-assist windowだけ一定間隔で取得し、primaryは充電開始/完了イベントだけにする。
    "fleet_day_poll_enabled": True,
    "fleet_day_poll_start": env_str("FLEET_PLANNING_POLL_START", ""),
    "fleet_day_poll_end": env_str("FLEET_PLANNING_POLL_END", ""),
    "fleet_day_poll_interval_sec": 300,
    "fleet_poll_only_when_wall_connected": True,
    "fleet_fetch_on_charging_stop": True,
    # TeslaMate MQTT取得コードは残すが、デフォルトでは呼び出さない。
    "teslamate_mqtt_enabled": False,
    "tesla_range_use_teslamate_live": False,
    # 充電→停止→すぐ空調のような連続操作で、古いFleet分類を引きずらないための安全設定。
    "wall_low_power_reclassify_sec": 10,
    "wall_no_activity_reset_sec": 10,

    # Tesla SOC/km表示はFleet分類時のlast-known値を使う。定期Fleet取得はしない。
    "tesla_range_cache_file": "tesla_range_cache.json",
    "tesla_range_stale_hours": 24,
    "tesla_range_wall_estimate_enabled": True,
    # Model 3実測寄りの概算。Wall Connector Whを航続距離へ換算する表示用係数。
    "tesla_range_wh_per_km": 145,
    "tesla_range_wall_charge_efficiency": 0.92,
    "tesla_estimated_usable_capacity_kwh": 54.0,
}
app_logs = []
debug_logs = []
g_last_debug_log_time = None

def add_log(event, action_text, level="info"):
    now = datetime.now(JST)
    now_str = now.strftime("%H:%M:%S")
    log_entry = {"time": now_str, "event": event, "action": action_text, "level": level}
    app_logs.insert(0, log_entry) 
    if len(app_logs) > 50: app_logs.pop()

    filename = f"SmartHEMS_log{now.strftime('%Y%m%d')}.txt"
    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(f"[{now_str}] [{level.upper()}] {event} :: {action_text}\n")
    except: pass


def add_debug(event, detail, data=None, throttle_sec=0):
    """デバッグログ。settings['debug_mode'] がONのときだけUIへ出す。"""
    global g_last_debug_log_time
    if not settings.get("debug_mode", False):
        return

    now = datetime.now(JST)
    if throttle_sec and g_last_debug_log_time and (now - g_last_debug_log_time).total_seconds() < throttle_sec:
        return
    if throttle_sec:
        g_last_debug_log_time = now

    entry = {
        "time": now.strftime("%H:%M:%S"),
        "event": event,
        "detail": detail,
        "data": data or {}
    }
    debug_logs.insert(0, entry)
    if len(debug_logs) > 200:
        debug_logs.pop()


def send_discord_notify(msg):
    url = sys_settings.get("DISCORD_WEBHOOK_URL", "")
    if not url or "ここ" in url: return
    try: requests.post(url, json={"content": msg}, timeout=3)
    except: pass

# --- InfluxDB ---
INFLUX_URL = env_str("INFLUX_URL", "http://127.0.0.1:8086")
INFLUX_TOKEN = env_str("INFLUX_TOKEN", "")
INFLUX_ORG = env_str("INFLUX_ORG", "")
INFLUX_BUCKET = env_str("INFLUX_BUCKET", "")

# InfluxDB settings are configured directly below.
# If token is empty, control continues and only metric writes are skipped.
client = None
write_api = None
if INFLUX_URL and INFLUX_TOKEN and INFLUX_ORG and INFLUX_BUCKET:
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
    except Exception as e:
        print(f"🚨 InfluxDB初期化エラー: {e}")

# ==========================================
# ⚡ テスラ操作 ＆ ブレーカー保護 ＆ 余剰充電
# ==========================================


def parse_hhmm_to_minutes(value, default_minutes=0):
    """HH:MM文字列を0:00からの分へ変換する。"""
    try:
        if isinstance(value, (int, float)):
            return int(value)
        parts = str(value).strip().split(":")
        if len(parts) != 2:
            return int(default_minutes)
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return int(default_minutes)


def is_minutes_in_window(now_minutes, start_minutes, end_minutes):
    """日跨ぎにも対応した分ベースの時間帯判定。"""
    if start_minutes <= end_minutes:
        return start_minutes <= now_minutes < end_minutes
    return now_minutes >= start_minutes or now_minutes < end_minutes


def parse_contract_window(setting_key):
    """ローカル設定の時間帯文字列を分単位へ変換する。公開版では具体時刻を持たない。"""
    raw = settings.get(setting_key) or sys_settings.get(setting_key) or ""
    if isinstance(raw, dict):
        start = raw.get("start")
        end = raw.get("end")
        if start and end:
            return parse_hhmm_to_minutes(start), parse_hhmm_to_minutes(end)
        return None
    raw = str(raw).strip()
    if not raw or raw.lower() in ("disabled", "off", "none"):
        return None
    sep = "-" if "-" in raw else "〜" if "〜" in raw else "~" if "~" in raw else None
    if not sep:
        return None
    start, end = [x.strip() for x in raw.split(sep, 1)]
    return parse_hhmm_to_minutes(start), parse_hhmm_to_minutes(end)


def is_configured_window(setting_key, now=None):
    now = now or datetime.now(JST)
    win = parse_contract_window(setting_key)
    if not win:
        return False
    return is_minutes_in_window(now.hour * 60 + now.minute, win[0], win[1])


def is_primary_charge_window(now=None):
    return is_configured_window("primary_charge_window", now)


def is_solar_assist_charge_window(now=None):
    return is_configured_window("solar_assist_charge_window", now)


def is_cheap_charge_window(now=None):
    """契約上の充電優先時間帯か。具体時刻はローカル設定で管理する。"""
    now = now or datetime.now(JST)
    return is_primary_charge_window(now) or is_solar_assist_charge_window(now)


def get_window_end_hour_float(setting_key, now=None, fallback=None):
    now = now or datetime.now(JST)
    win = parse_contract_window(setting_key)
    if not win:
        return fallback if fallback is not None else (now.hour + now.minute / 60.0)
    start, end = win
    current = now.hour * 60 + now.minute + now.second / 60.0
    end_abs = float(end)
    if start > end and current >= start:
        end_abs += 24 * 60
    if start <= end and current > end:
        return fallback if fallback is not None else (now.hour + now.minute / 60.0)
    return end_abs / 60.0


def hours_until_window_end(setting_key, now=None, minimum=0.05):
    now = now or datetime.now(JST)
    end_h = get_window_end_hour_float(setting_key, now, fallback=(now.hour + now.minute / 60.0))
    cur_h = now.hour + now.minute / 60.0 + now.second / 3600.0
    return max(float(minimum), end_h - cur_h)


def get_window_end_datetime(setting_key, now=None):
    """設定された時間帯の終了時刻をdatetimeで返す。未設定時は現在時刻を返す。"""
    now = now or datetime.now(JST)
    win = parse_contract_window(setting_key)
    if not win:
        return now
    start, end = win
    end_dt = now.replace(hour=int(end // 60) % 24, minute=int(end % 60), second=0, microsecond=0)
    current_min = now.hour * 60 + now.minute
    # 日跨ぎwindowまたはすでに終了後なら次回終了時刻へ送る。
    if start > end and current_min >= start:
        end_dt += timedelta(days=1)
    elif start <= end and current_min >= end:
        end_dt += timedelta(days=1)
    return end_dt


def minutes_until_solar_window_end(now=None):
    return hours_until_window_end("solar_assist_charge_window", now, minimum=0.0) * 60.0


def is_solar_window_early_phase(now=None):
    now = now or datetime.now(JST)
    win = parse_contract_window("solar_assist_charge_window")
    if not win:
        return False
    start, _ = win
    current = now.hour * 60 + now.minute
    early = int(settings.get("day_early_tesla_priority_until_minute", 10))
    return is_minutes_in_window(current, start, start + early)


def is_window_end_recovery_due(setting_key, now=None, grace_minutes=10):
    now = now or datetime.now(JST)
    win = parse_contract_window(setting_key)
    if not win:
        return False
    _, end = win
    current = now.hour * 60 + now.minute
    if current < end:
        current += 24 * 60
    return 0 <= (current - end) < int(grace_minutes)


def is_after_solar_window(now=None):
    now = now or datetime.now(JST)
    win = parse_contract_window("solar_assist_charge_window")
    if not win:
        return False
    _, end = win
    current = now.hour * 60 + now.minute
    return current >= end



def is_planned_tesla_charge_window(now=None):
    """
    Teslaを積極的に充電開始/継続したい時間帯。

    重要:
      この時間帯では、Chargingではない待機状態でも5Aリセットを行わない。
      solar_window_start開始時点やprimary開始時点で、32Aスタートを邪魔しないため。
    """
    return is_cheap_charge_window(now)


def is_tesla_aux_power_active():
    """
    TeslaがCharging/Startingではないのに、充電器経由で空調・温調・補機電力を使っている状態を検出する。

    例:
      - plugged_in=True
      - charging_state=Stopped/Complete など
      - charger_power が0.3kW以上

    この状態は通常充電ではないが、ブレーカー保護上はTesla系負荷として扱う。
    """
    try:
        # Wall Connectorを主センサーにする。charging以外のWall負荷は空調/補機扱い。
        if wall_connector_indicates_tesla_load():
            return not wall_connector_indicates_charging_load()

        if not tesla_state.get("plugged_in", False):
            return False

        if not can_control_tesla_now():
            return False

        charging_state = tesla_state.get("charging_state", "")
        if charging_state in ["Charging", "Starting"]:
            return False

        charger_kw = float(tesla_state.get("charger_power", tesla_state.get("power", 0)) or 0)
        threshold_kw = float(settings.get("tesla_aux_power_detect_kw", 0.3))
        return charger_kw >= threshold_kw
    except Exception:
        return False


def should_reset_tesla_idle_amps(now=None):
    """
    Teslaが充電中ではない時の残留A設定を安全側へ戻すべきか。

    方針:
      - configured contract charge windows は、これから充電開始する可能性があるので戻さない。
      - それ以外の時間帯で、Complete/Stopped または補機電力中なら5Aへ戻す。
      - これにより、solar window start開始が5Aになってしまう問題を避けつつ、
        solar window end以降などの待機時32A残留を潰す。
    """
    now = now or datetime.now(JST)

    if not settings.get("tesla_idle_safe_amps_reset_enabled", True):
        return False
    if is_planned_tesla_charge_window(now):
        return False
    if not tesla_state.get("plugged_in", False):
        return False
    if not can_control_tesla_now():
        return False

    charging_state = tesla_state.get("charging_state", "")
    if charging_state in ["Charging", "Starting"]:
        return False

    return charging_state in ["Complete", "Stopped", "NoPower", "Disconnected"] or is_tesla_aux_power_active()


def reset_tesla_idle_amps_if_needed(now=None):
    """
    安価充電時間帯外で、Teslaが充電中ではないのに大きなA設定を残している場合、
    5Aなどの安全Aへ戻す。

    force=Trueで送るが、should_reset_tesla_idle_amps() が安価時間帯を除外するため、
    solar_window_start開始時点の32Aスタートは邪魔しない。
    """
    global g_last_tesla_idle_reset_log_time, g_last_tesla_critical_drop_time

    now = now or datetime.now(JST)
    if not should_reset_tesla_idle_amps(now):
        return False

    idle_safe_amps = int(settings.get("tesla_idle_safe_amps", 5))
    reported_amps = int(tesla_state.get("request_current", 0) or tesla_state.get("actual_current", 0) or 0)
    if reported_amps <= idle_safe_amps:
        return False

    if request_tesla_amps_change(idle_safe_amps, force=True, reason="idle_aux_safety"):
        if (
            g_last_tesla_idle_reset_log_time is None
            or (now - g_last_tesla_idle_reset_log_time).total_seconds() >= 300
        ):
            add_log(
                "Tesla待機電流リセット",
                f"安価充電時間帯外で充電中ではないため、残留A設定を {reported_amps}A ➔ {idle_safe_amps}A に下げました",
                "warning"
            )
            g_last_tesla_idle_reset_log_time = now
        return True

    return False


def build_tesla_aux_status(now=None):
    """
    Teslaの補機/空調電力・待機Aリセット・SmartCharge抑制状態をUI/APIへ見える化する。

    目的:
      - Chargingではないが充電器経由で空調/温調電力を使っている状態を表示する
      - 安価時間帯外で32Aなどの残留設定を5Aへ戻す対象かを表示する
      - solar assist window / primary charge windowは、開始前の32A準備を邪魔しないためリセット対象外であることを表示する
      - ブレーカー保護対象に含めているかを表示する
    """
    now = now or datetime.now(JST)
    try:
        charging_state = str(tesla_state.get("charging_state", "unknown"))
        plugged = bool(tesla_state.get("plugged_in", False))
        charger_kw = float(tesla_state.get("charger_power", tesla_state.get("power", 0)) or 0)
        planned_window = bool(is_planned_tesla_charge_window(now))
        home_control = bool(can_control_tesla_now())
        aux_active = bool(is_tesla_aux_power_active())
        reset_allowed = bool(should_reset_tesla_idle_amps(now))
        idle_safe_amps = int(settings.get("tesla_idle_safe_amps", 5))
        reported_amps = int(tesla_state.get("request_current", 0) or tesla_state.get("actual_current", 0) or 0)
        charging = charging_state in ["Charging", "Starting"] and bool(tesla_state.get("home_charging", False))
        breaker_guard_target = bool(charging or aux_active)
        smartcharge_start_suppressed = bool(aux_active and not planned_window)

        if charging:
            label = "Tesla自宅充電中"
            detail = "通常充電としてA制御・ブレーカー保護の対象です。"
            level = "info"
        elif aux_active and planned_window:
            label = "Tesla補機/空調電力中（計画充電時間帯）"
            detail = "充電器経由の電力を検出。solar assist window/primary charge windowは32A開始を邪魔しないため待機Aリセットは行いません。ブレーカー保護対象です。"
            level = "warning"
        elif aux_active:
            label = "Tesla補機/空調電力中"
            detail = "Chargingではありませんが充電器経由の電力を検出。SmartCharge開始は抑制し、ブレーカー保護対象として扱います。"
            level = "warning"
        elif reset_allowed and reported_amps > idle_safe_amps:
            label = "Tesla待機Aリセット対象"
            detail = f"安価時間帯外で充電中ではないため、残留A設定 {reported_amps}A を {idle_safe_amps}A へ戻す対象です。"
            level = "warning"
        elif plugged and not planned_window and charging_state not in ["Charging", "Starting"]:
            label = "Tesla接続待機中"
            detail = f"安価時間帯外の待機状態です。残留A設定は {idle_safe_amps}A へ戻す方針です。"
            level = "info"
        elif plugged and planned_window and charging_state not in ["Charging", "Starting"]:
            label = "Tesla接続待機中（計画充電時間帯）"
            detail = "これから充電開始する可能性があるため、待機Aリセットは行いません。開始条件を満たせば32A上限で充電します。"
            level = "info"
        else:
            label = "Tesla通常監視"
            detail = "補機/空調電力や待機Aリセット対象は検出していません。"
            level = "info"

        return {
            "active": bool(aux_active or reset_allowed or charging),
            "level": level,
            "label": label,
            "detail": detail,
            "aux_power_active": aux_active,
            "aux_power_kw": round(charger_kw, 2),
            "aux_power_w": int(round(charger_kw * 1000)),
            "planned_charge_window": planned_window,
            "idle_safe_reset_allowed": reset_allowed,
            "idle_safe_reset_pending": bool(reset_allowed and reported_amps > idle_safe_amps),
            "idle_safe_amps": idle_safe_amps,
            "reported_amps": reported_amps,
            "smartcharge_start_suppressed": smartcharge_start_suppressed,
            "breaker_guard_target": breaker_guard_target,
            "home_control_allowed": home_control,
            "charging_state": charging_state,
            "plugged_in": plugged,
            "time": now.strftime("%H:%M:%S"),
        }
    except Exception as e:
        return {
            "active": False,
            "level": "warning",
            "label": "Tesla補機/待機状態取得エラー",
            "detail": str(e),
            "aux_power_active": False,
            "aux_power_kw": 0.0,
            "aux_power_w": 0,
            "planned_charge_window": False,
            "idle_safe_reset_allowed": False,
            "idle_safe_reset_pending": False,
            "idle_safe_amps": int(settings.get("tesla_idle_safe_amps", 5)),
            "reported_amps": 0,
            "smartcharge_start_suppressed": False,
            "breaker_guard_target": False,
            "home_control_allowed": False,
            "charging_state": str(tesla_state.get("charging_state", "unknown")),
            "plugged_in": bool(tesla_state.get("plugged_in", False)),
            "time": now.strftime("%H:%M:%S"),
        }


def get_tesla_amp_cooldown_sec(diff_a, now=None):
    """
    Tesla A変更の待機秒数を、時間帯と差分で決める。

    方針:
      - 安価時間帯は最大化寄りなので上げ方向も早め
      - 通常時間帯は慎重
      - 差分が大きいほど早く許可
      - ブレーカー保護は force=True 側で即時
    """
    now = now or datetime.now(JST)
    abs_diff = abs(int(diff_a))
    cheap = is_cheap_charge_window(now)

    if abs_diff >= int(settings.get("tesla_amp_very_large_diff_a", 8)):
        return int(settings.get("tesla_amp_very_large_diff_cooldown_sec", 15))

    if abs_diff >= int(settings.get("tesla_amp_large_diff_a", 4)):
        return int(settings.get("tesla_amp_large_diff_cooldown_sec", 30))

    if cheap:
        if diff_a > 0:
            return int(settings.get("tesla_amp_cheap_increase_cooldown_sec", 45))
        return int(settings.get("tesla_amp_cheap_decrease_cooldown_sec", 45))

    if diff_a > 0:
        return int(settings.get("tesla_amp_increase_cooldown_sec", 120))
    return int(settings.get("tesla_amp_decrease_cooldown_sec", 60))


def should_send_tesla_amp_command(target_amps, force=False, reason=""):
    """
    Tesla A変更の最終出口ガード。

    fix8:
      統合時に g_last_tesla_commanded_time / amps が未定義になるケースへ対応。
      関数内でも globals() を自己修復する。
    """
    global g_last_tesla_commanded_amps, g_last_tesla_commanded_time

    if "g_last_tesla_commanded_amps" not in globals():
        g_last_tesla_commanded_amps = None
    if "g_last_tesla_commanded_time" not in globals():
        g_last_tesla_commanded_time = None

    now = datetime.now(JST)
    target_amps = int(target_amps)

    # Wall Connector実測Aを優先して、TeslaMate/Fleetの反映遅延でA変更を抑止しすぎない。
    try:
        current_reported = int(get_tesla_control_current_amps() or 0)
    except Exception:
        current_reported = int(tesla_state.get("request_current", 0) or tesla_state.get("actual_current", 0) or 0)
    effective_current = current_reported

    # 直近送信Aは、TeslaMate/API反映遅延中の実効現在Aとして扱う。
    if g_last_tesla_commanded_amps is not None and g_last_tesla_commanded_time is not None:
        if (now - g_last_tesla_commanded_time).total_seconds() < 180:
            effective_current = int(g_last_tesla_commanded_amps)

    if force:
        g_last_tesla_commanded_amps = target_amps
        g_last_tesla_commanded_time = now
        return True

    deadband_a = max(1, int(settings.get("tesla_amp_change_deadband_a", 2)))
    diff = target_amps - effective_current

    # 小さい差分は送らない
    if abs(diff) < deadband_a:
        return False

    required_cd = get_tesla_amp_cooldown_sec(diff, now) if "get_tesla_amp_cooldown_sec" in globals() else (
        int(settings.get("tesla_amp_cheap_increase_cooldown_sec", 45)) if is_cheap_charge_window(now)
        else int(settings.get("tesla_amp_increase_cooldown_sec", 120))
    )

    # 直近送信からの間隔制限
    if g_last_tesla_commanded_time is not None:
        elapsed = (now - g_last_tesla_commanded_time).total_seconds()
        if elapsed < required_cd:
            return False

    g_last_tesla_commanded_amps = target_amps
    g_last_tesla_commanded_time = now

    tesla_state["last_amp_gate"] = {
        "target": target_amps,
        "effective_current": effective_current,
        "diff": diff,
        "cooldown_sec": required_cd,
        "cheap_window": is_cheap_charge_window(now),
        "reason": reason,
        "time": now.strftime("%H:%M:%S"),
    }

    return True


def request_tesla_command(action, value=None, force=False, reason=""):
    """
    Tesla制御コマンドの最終出口。

    ここで必ず:
      - 自宅判定ガード
      - Tesla A変更のクールダウン/デッドバンド
      - 非充電時のA変更抑止
    を通す。
    """
    global g_last_tesla_amp_suppress_log_time

    # Tesla A変更は「自宅で充電中」と確定している時だけ送る。
    # set_amps は停止中にも通信できてしまうため、Battery Firstの目安計算やUI更新だけで
    # Teslaへ不要な通信が飛ばないように最終出口で止める。
    if action == "tesla" and value is not None and not force:
        try:
            total = estimate_total_home_consumption_w()
            amp_active = tesla_amp_control_active(total)
            charging_state = tesla_state.get("charging_state", "")
            wall_load_type = ev_wall_state.get("load_type")
            wall_power_w = int(float(ev_wall_state.get("power_w") or 0))
            if not amp_active:
                now_for_log = datetime.now(JST)
                if (
                    g_last_tesla_amp_suppress_log_time is None
                    or (now_for_log - g_last_tesla_amp_suppress_log_time).total_seconds() >= 300
                ):
                    add_log(
                        "Tesla A変更抑止",
                        f"TeslaがA変更対象の充電状態ではないため {value}A は送信しません: state={charging_state}, wall={wall_load_type}/{wall_power_w}W, reason={tesla_state.get('home_charge_reason')}",
                        "info"
                    )
                    g_last_tesla_amp_suppress_log_time = now_for_log
                return False
        except Exception as e:
            add_log("Tesla A変更抑止エラー", str(e), "warning")
            return False

    # Teslaへのstart/stop/amp変更は、自宅にいると確定できた時だけ許可。
    if str(action).startswith("tesla") or action == "tesla":
        if not can_control_tesla_now():
            add_log("Tesla制御抑止", f"自宅にいると確定できないため {action} を送信しません: {tesla_state.get('home_charge_reason')}", "warning")
            return False

    # action=="tesla" はアンペア変更。どの経路から来てもここで抑止する。
    if action == "tesla" and value is not None:
        try:
            target_amps = int(value)
            if not should_send_tesla_amp_command(target_amps, force=force, reason=reason):
                return False
        except Exception as e:
            add_log("Tesla A変更抑止エラー", str(e), "warning")
            return False

    payload = {"action": action}
    if value is not None:
        payload["value"] = value

    try:
        r = requests.post(post_control_url("/api/command"), json=payload, timeout=5)
        return 200 <= r.status_code < 300
    except Exception as e:
        add_log("Tesla制御失敗", f"{action} 送信失敗: {e}", "warning")
        return False

def request_tesla_amps_change(target_amps, force=False, reason=""):
    return request_tesla_command("tesla", int(target_amps), force=force, reason=reason)


def wall_connector_url(path="/api/1/vitals"):
    ip = str(settings.get("wall_connector_ip", env_str("WALL_CONNECTOR_IP", ""))).strip()
    if not ip:
        raise RuntimeError("WALL_CONNECTOR_IP is not configured")
    return f"http://{ip}{path}"


def wall_ev_power_active(power_w):
    return float(power_w or 0) >= float(settings.get("wall_power_on_w", 200))


def wall_ev_power_inactive(power_w):
    return float(power_w or 0) <= float(settings.get("wall_power_off_w", 100))


def wall_ev_low_power_present(power_w, current_a, contactor_closed):
    """
    200W未満でも、空調/温調/補機電力としてWall Connector経由の負荷が
    継続している可能性がある状態。

    これはPOST側へ「充電中」と通知する条件ではない。
    あくまでREAD側がclimate_only等を維持・再分類するための検出。
    """
    try:
        return (
            bool(contactor_closed)
            and float(power_w or 0) >= float(settings.get("wall_activity_min_w", 50))
            and float(current_a or 0) >= float(settings.get("wall_activity_min_a", 0.2))
        )
    except Exception:
        return False


def wall_ev_activity_present(power_w, current_a, contactor_closed):
    return wall_ev_power_active(power_w) or wall_ev_low_power_present(power_w, current_a, contactor_closed)


def fleet_classification_is_charging(load_type):
    return str(load_type) == "charging"


def wall_connector_should_notify_charging():
    """
    POST側の既存 update_tesla_state へTrueを送ってよいか。

    Fleet分類がchargingでも、Wall Connector実電力がon閾値未満に落ちたら
    充電中通知は即Falseにする。
    充電停止直後に空調を入れた場合、古いFleet分類を引きずって
    POST側をStandby防衛させないため。
    """
    if not ev_wall_state.get("load_active"):
        return False
    if not wall_ev_power_active(ev_wall_state.get("power_w", 0)):
        return False
    return fleet_classification_is_charging(ev_wall_state.get("load_type"))





def wall_connector_indicates_home_connected():
    """Wall ConnectorのローカルAPIが取れて車両接続中なら、自宅接続とみなす。

    TeslaMateを無効化していても、自宅固定IPのWall Connectorは外部充電器と混同しない。
    start/stop/ampsを送る前の安全なhome判定として使う。
    """
    try:
        if not settings.get("tesla_control_allow_wallconnector_home", True):
            return False
        return bool(
            ev_wall_state.get("api_ok")
            and ev_wall_state.get("vehicle_connected")
        )
    except Exception:
        return False


def wall_connector_indicates_tesla_load():
    """Wall Connector上でTesla系負荷が動いているか。充電/空調/補機を含む。"""
    try:
        if not (ev_wall_state.get("api_ok") and ev_wall_state.get("vehicle_connected")):
            return False
        power_w = _safe_float(ev_wall_state.get("power_w"), 0.0) or 0.0
        current_a = _safe_float(ev_wall_state.get("current_a"), 0.0) or 0.0
        contactor = bool(ev_wall_state.get("contactor_closed"))
        return bool(ev_wall_state.get("load_active")) or wall_ev_activity_present(power_w, current_a, contactor)
    except Exception:
        return False


def wall_connector_indicates_charging_load():
    """Wall Connector + Fleet分類で、制御対象の充電負荷かを判断する。"""
    try:
        if not wall_connector_indicates_tesla_load():
            return False
        return fleet_classification_is_charging(ev_wall_state.get("load_type"))
    except Exception:
        return False


def tesla_amp_control_active(total_consumption_w=None):
    """TeslaのA変更対象として扱ってよい充電状態か。

    v17:
      Fleet/TeslaMate側の charging_state は反映遅延やFleet取得失敗で
      Stopped のまま残ることがある。
      その場合でも、自宅Wall Connectorが charging load を示していれば、
      A変更対象として扱う。

    これにより:
      - Battery First中の「Tesla未充電」誤判定
      - request_tesla_command出口の state=Stopped によるA変更抑止
      - ブレーカー保護の repeated drop が同じAへ繰り返し送る問題
    を避ける。
    """
    try:
        if wall_connector_indicates_charging_load():
            tesla_state["home_charge_reason"] = "wall_connector_charging"
            tesla_state["home_charging"] = True
            return True

        charging_state = tesla_state.get("charging_state", "")
        if charging_state in ["Charging", "Starting"]:
            if tesla_state.get("home_charging", False):
                return True
            try:
                return bool(is_tesla_home_charging(total_consumption_w))
            except Exception:
                return False
        return False
    except Exception:
        return False


def get_tesla_breaker_effective_current_amps(current_amps=None):
    """ブレーカー保護の降下計算に使うA。

    Wall実測Aは反映が遅れることがあるため、直近に31Aへ下げた直後に
    Wallがまだ32Aを返しても、次回criticalでは31Aを基準にさらに下げる。
    これをしないと 32A→31A を何度も繰り返し、6.4kW超過が続く。
    """
    try:
        now = datetime.now(JST)
        actual_a = int(current_amps or get_tesla_control_current_amps() or 0)
        commanded = globals().get("g_last_tesla_commanded_amps", None)
        commanded_time = globals().get("g_last_tesla_commanded_time", None)
        if commanded is not None and commanded_time is not None:
            age = (now - commanded_time).total_seconds()
            if 0 <= age <= int(settings.get("tesla_breaker_commanded_floor_sec", 120)):
                # 降下方向では、直近指令値を床として扱う。
                # 例: Wall実測32Aのままでも、前回31Aを送っていたら次は31A基準で30Aへ落とせる。
                if actual_a <= 0:
                    return int(commanded)
                return int(min(actual_a, int(commanded)))
        return int(actual_a)
    except Exception:
        try:
            return int(current_amps or 0)
        except Exception:
            return 0


def get_wall_connector_current_a():
    try:
        if not settings.get("wall_connector_current_as_control_source", True):
            return 0
        if not (ev_wall_state.get("api_ok") and ev_wall_state.get("vehicle_connected")):
            return 0
        a = _safe_float(ev_wall_state.get("current_a"), 0.0) or 0.0
        if a <= 0:
            return 0
        # TeslaのA設定は整数なので、実測Aは切り上げて安全側にする。
        return int(math.ceil(a))
    except Exception:
        return 0


def get_tesla_control_current_amps():
    """Tesla電流制御に使う現在A。

    優先順位:
      1. Wall Connector実測 current_a
      2. 直近にHEMSが命令したA
      3. Tesla/Fleet/TeslaMate由来のrequest/actual current
      4. 充電負荷があるがA不明なら最低A
    """
    try:
        wall_a = get_wall_connector_current_a()
        if wall_a > 0:
            return wall_a

        if g_last_tesla_commanded_amps is not None:
            return int(g_last_tesla_commanded_amps)

        state_a = max(
            int(float(tesla_state.get("request_current", 0) or 0)),
            int(float(tesla_state.get("actual_current", 0) or 0)),
        )
        if state_a > 0:
            return state_a

        if wall_connector_indicates_tesla_load():
            return int(settings.get("solar_priority_min_amps", 5))
    except Exception:
        pass
    return 0

def estimate_load_source_split(home_w, tesla_w):
    """
    UI表示用の推定配分。

    物理的にはAC母線上で電力は混ざるため厳密な由来断定ではない。
    HEMS画面としては「自宅負荷を先に満たし、残りをTeslaへ流す」
    という読み方で、Solar -> Battery discharge -> Grid の順に配分する。
    """
    try:
        home_w = max(0.0, float(home_w or 0.0))
        tesla_w = max(0.0, float(tesla_w or 0.0))
        pools = {
            "solar_w": max(0.0, float(state.get("solar", 0) or 0.0)),
            "battery_w": max(0.0, -float(state.get("battery", 0) or 0.0)),
            "grid_w": max(0.0, float(state.get("grid", 0) or 0.0)),
        }

        def alloc(load_w):
            rem = max(0.0, float(load_w or 0.0))
            out = {"solar_w": 0.0, "battery_w": 0.0, "grid_w": 0.0}
            for key in ["solar_w", "battery_w", "grid_w"]:
                take = min(rem, pools[key])
                out[key] += take
                pools[key] -= take
                rem -= take
                if rem <= 0.0001:
                    break
            # メーター更新タイミング差などで供給プールが不足する場合は、表示上はGrid由来に寄せる
            if rem > 0.0001:
                out["grid_w"] += rem
            return out

        home = alloc(home_w)
        tesla = alloc(tesla_w)
        total_parts = {
            "solar_w": home.get("solar_w", 0.0) + tesla.get("solar_w", 0.0),
            "battery_w": home.get("battery_w", 0.0) + tesla.get("battery_w", 0.0),
            "grid_w": home.get("grid_w", 0.0) + tesla.get("grid_w", 0.0),
        }
        total_load_w = home_w + tesla_w

        def finalize(total_w, parts):
            total_w = max(0.0, float(total_w or 0.0))
            denom = total_w if total_w > 0 else 1.0
            return {
                "total_w": round(total_w, 1),
                "solar_w": round(parts.get("solar_w", 0.0), 1),
                "battery_w": round(parts.get("battery_w", 0.0), 1),
                "grid_w": round(parts.get("grid_w", 0.0), 1),
                "solar_pct": round((parts.get("solar_w", 0.0) / denom) * 100, 1) if total_w > 0 else 0.0,
                "battery_pct": round((parts.get("battery_w", 0.0) / denom) * 100, 1) if total_w > 0 else 0.0,
                "grid_pct": round((parts.get("grid_w", 0.0) / denom) * 100, 1) if total_w > 0 else 0.0,
            }

        return {
            "mode": "estimated_home_first",
            "note": "Estimated allocation: Home load first, then Tesla; Solar -> Battery discharge -> Grid.",
            "sources_available_w": {
                "solar_w": round(max(0.0, float(state.get("solar", 0) or 0.0)), 1),
                "battery_w": round(max(0.0, -float(state.get("battery", 0) or 0.0)), 1),
                "grid_w": round(max(0.0, float(state.get("grid", 0) or 0.0)), 1),
            },
            "total": finalize(total_load_w, total_parts),
            "home": finalize(home_w, home),
            "tesla": finalize(tesla_w, tesla),
        }
    except Exception as e:
        return {
            "mode": "error",
            "note": str(e),
            "sources_available_w": {"solar_w": 0, "battery_w": 0, "grid_w": 0},
            "total": {"total_w": 0, "solar_w": 0, "battery_w": 0, "grid_w": 0, "solar_pct": 0, "battery_pct": 0, "grid_pct": 0},
            "home": {"total_w": 0, "solar_w": 0, "battery_w": 0, "grid_w": 0, "solar_pct": 0, "battery_pct": 0, "grid_pct": 0},
            "tesla": {"total_w": 0, "solar_w": 0, "battery_w": 0, "grid_w": 0, "solar_pct": 0, "battery_pct": 0, "grid_pct": 0},
        }



def _parse_jst_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S').replace(tzinfo=JST)
    except Exception:
        try:
            return datetime.fromisoformat(str(value)).astimezone(JST)
        except Exception:
            return None


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


MILES_TO_KM = 1.609344


def _miles_to_km(value, default=None):
    v = _safe_float(value, None)
    if v is None:
        return default
    return v * MILES_TO_KM


def _pick_first_float(mapping, keys, default=None):
    if not isinstance(mapping, dict):
        return default
    for key in keys:
        v = _safe_float(mapping.get(key), None)
        if v is not None and v > 0:
            return v
    return default


def _pick_fleet_range_km(charge_state):
    """Fleet vehicle_dataのrangeはmileで返るため、kmへ変換して使う。

    POST側fleet_state_json.py v2以降なら *_range_km が既に入る。
    旧POSTや生Fleet値なら *_range / *_range_mi をmileとして扱う。
    """
    range_km = _pick_first_float(charge_state, [
        "battery_range_km",
        "est_battery_range_km",
        "ideal_battery_range_km",
    ], None)
    if range_km is not None:
        return range_km, "fleet_battery_range_km_cached"

    range_mi = _pick_first_float(charge_state, [
        "battery_range_mi",
        "battery_range",
        "est_battery_range_mi",
        "est_battery_range",
        "ideal_battery_range_mi",
        "ideal_battery_range",
    ], None)
    if range_mi is not None:
        return range_mi * MILES_TO_KM, "fleet_battery_range_mi_cached"

    return None, None


def _infer_effective_max_range_km(range_km, soc_percent, fallback=397.0):
    try:
        if range_km is None or soc_percent is None or soc_percent <= 0:
            return fallback
        inferred = float(range_km) / (float(soc_percent) / 100.0)
        # Model 3 SR+としてあり得る範囲に限定。異常値なら既存値へfallback。
        if 250.0 <= inferred <= 600.0:
            return inferred
    except Exception:
        pass
    return fallback


def _estimated_remaining_kwh_from_soc(soc_percent):
    """残量kWhがFleet vehicle_dataで返らない場合の参考推定値。

    Tesla APIのenergy_remainingが無い環境向け。表示時はestimated扱い。
    """
    try:
        capacity = float(settings.get("tesla_estimated_usable_capacity_kwh", 54.0))
        soc = float(soc_percent)
        return max(0.0, capacity * soc / 100.0)
    except Exception:
        return None


def _range_cache_path():
    return str(settings.get("tesla_range_cache_file", "tesla_range_cache.json"))


def load_tesla_range_cache():
    """Tesla SOC/kmのlast-known cacheを読む。失敗しても制御には影響させない。"""
    global g_tesla_range_cache
    try:
        path = _range_cache_path()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                g_tesla_range_cache.update(loaded)
    except Exception as e:
        add_debug("Tesla range cache load failed", str(e), throttle_sec=300)
    return g_tesla_range_cache


def save_tesla_range_cache():
    """Tesla SOC/kmのlast-known cacheを保存。Wall 5秒周期では頻繁に書かない。"""
    try:
        with open(_range_cache_path(), "w", encoding="utf-8") as f:
            json.dump(g_tesla_range_cache, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        add_debug("Tesla range cache save failed", str(e), throttle_sec=300)
        return False


def update_tesla_range_cache_from_fleet(data, now=None):
    """
    Fleet分類/計画取得時に取れたSOC/rangeをlast-knownとして保存する。

    重要:
      - これ自体はFleet APIを呼ばない。
      - 既存のEV負荷分類/昼計画ポーリングで取得済みのdataを再利用するだけ。
      - Fleetの battery_range / ideal_battery_range はmileなのでkmへ変換して保存する。
    """
    global g_tesla_range_cache
    now = now or datetime.now(JST)
    if not isinstance(data, dict):
        return False

    charge_state = data.get("charge_state") or {}
    soc = _safe_float(charge_state.get("battery_level"), None)
    if soc is None or soc <= 0:
        return False

    usable_soc = _safe_float(charge_state.get("usable_battery_level"), None)

    fallback_max_range = _safe_float(tesla_state.get("max_range"), 397.0) or 397.0
    if fallback_max_range <= 0:
        fallback_max_range = 397.0

    charge_limit = _safe_float(
        charge_state.get("charge_limit_soc") or tesla_state.get("charge_limit"),
        100.0,
    ) or 100.0
    if charge_limit <= 0:
        charge_limit = 100.0

    fleet_range_km, fleet_range_source = _pick_fleet_range_km(charge_state)

    tm_range = _safe_float(tesla_state.get("range"), None)
    use_tm_live = bool(settings.get("tesla_range_use_teslamate_live", False))
    if use_tm_live and tm_range is not None and tm_range > 0:
        range_km = tm_range
        source = "tesla_state"
        is_estimated = False
        max_range = fallback_max_range
    elif fleet_range_km is not None and fleet_range_km > 0:
        range_km = fleet_range_km
        source = fleet_range_source or "fleet_range_cached"
        is_estimated = False
        # Teslaアプリ表示に合わせるため、現在rangeとSOC%から有効Rated maxを逆算する。
        max_range = _infer_effective_max_range_km(range_km, soc, fallback_max_range)
    else:
        max_range = fallback_max_range
        range_km = max_range * (soc / 100.0)
        source = "fleet_soc_cached_estimate"
        is_estimated = True

    target_range = max_range * (charge_limit / 100.0)
    missing = max(0.0, target_range - range_km)

    battery_range_mi = _safe_float(charge_state.get("battery_range_mi") or charge_state.get("battery_range"), None)
    ideal_range_mi = _safe_float(charge_state.get("ideal_battery_range_mi") or charge_state.get("ideal_battery_range"), None)
    est_range_mi = _safe_float(charge_state.get("est_battery_range_mi") or charge_state.get("est_battery_range"), None)
    battery_range_km = _safe_float(charge_state.get("battery_range_km"), None) or _miles_to_km(battery_range_mi, None)
    ideal_range_km = _safe_float(charge_state.get("ideal_battery_range_km"), None) or _miles_to_km(ideal_range_mi, None)
    est_range_km = _safe_float(charge_state.get("est_battery_range_km"), None) or _miles_to_km(est_range_mi, None)

    remaining_kwh = _safe_float(
        charge_state.get("energy_remaining_kwh")
        or charge_state.get("nominal_energy_remaining_kwh")
        or charge_state.get("energy_remaining")
        or charge_state.get("nominal_energy_remaining"),
        None,
    )
    remaining_kwh_est = _estimated_remaining_kwh_from_soc(soc)

    g_tesla_range_cache.update({
        "soc_percent": round(soc, 2),
        "usable_soc_percent": round(usable_soc, 2) if usable_soc is not None else None,
        "range_km": round(range_km, 3),
        "max_range_km": round(max_range, 3),
        "charge_limit_percent": round(charge_limit, 2),
        "target_range_km": round(target_range, 3),
        "missing_km": round(missing, 3),
        "source": source,
        "last_update": now.strftime('%Y-%m-%d %H:%M:%S'),
        "is_estimated": bool(is_estimated),
        "fleet_classification": data.get("classification"),
        "battery_range_mi": round(battery_range_mi, 3) if battery_range_mi is not None else None,
        "battery_range_km": round(battery_range_km, 3) if battery_range_km is not None else None,
        "ideal_battery_range_mi": round(ideal_range_mi, 3) if ideal_range_mi is not None else None,
        "ideal_battery_range_km": round(ideal_range_km, 3) if ideal_range_km is not None else None,
        "est_battery_range_mi": round(est_range_mi, 3) if est_range_mi is not None else None,
        "est_battery_range_km": round(est_range_km, 3) if est_range_km is not None else None,
        "remaining_kwh": round(remaining_kwh, 3) if remaining_kwh is not None else None,
        "remaining_kwh_estimated": round(remaining_kwh_est, 3) if remaining_kwh_est is not None else None,
        "remaining_kwh_is_estimated": remaining_kwh is None,
        "wall_estimate_active": False,
        "wall_estimate_started_at": None,
        "wall_estimate_start_session_wh": None,
        "wall_estimate_base_range_km": None,
        "wall_estimate_base_soc_percent": None,
    })
    save_tesla_range_cache()
    return True

def start_tesla_wall_range_estimate(now=None):
    """
    純粋なcharging分類時だけ、Wall Connector session_energy_whから航続距離の増加推定を開始する。
    charging_with_climate / climate_only では空調消費が混ざるため開始しない。
    """
    global g_tesla_range_cache
    if not settings.get("tesla_range_wall_estimate_enabled", True):
        return False
    now = now or datetime.now(JST)

    base_range = _safe_float(g_tesla_range_cache.get("range_km"), None)
    base_soc = _safe_float(g_tesla_range_cache.get("soc_percent"), None)
    if base_range is None or base_soc is None:
        return False
    if g_tesla_range_cache.get("wall_estimate_active"):
        return False

    g_tesla_range_cache.update({
        "wall_estimate_active": True,
        "wall_estimate_started_at": now.strftime('%Y-%m-%d %H:%M:%S'),
        "wall_estimate_start_session_wh": _safe_float(ev_wall_state.get("session_energy_wh"), 0.0) or 0.0,
        "wall_estimate_base_range_km": base_range,
        "wall_estimate_base_soc_percent": base_soc,
    })
    add_debug(
        "Tesla range wall estimate start",
        "Wall Connector session_energy_whから充電km推定を開始",
        {
            "base_range_km": base_range,
            "base_soc_percent": base_soc,
            "start_session_wh": g_tesla_range_cache.get("wall_estimate_start_session_wh"),
        },
    )
    return True


def _compute_wall_estimated_range(wall_payload=None):
    """現在のWall Connector Wh増加から、range/SOCの推定値を返す。保存はしない。"""
    wall_payload = wall_payload or build_wall_connector_payload()
    if not g_tesla_range_cache.get("wall_estimate_active"):
        return None
    try:
        base_range = float(g_tesla_range_cache.get("wall_estimate_base_range_km"))
        start_wh = float(g_tesla_range_cache.get("wall_estimate_start_session_wh"))
        current_wh = float(wall_payload.get("session_energy_wh") or ev_wall_state.get("session_energy_wh") or 0.0)
        added_wall_wh = max(0.0, current_wh - start_wh)
        efficiency = float(settings.get("tesla_range_wall_charge_efficiency", 0.92))
        wh_per_km = max(1.0, float(settings.get("tesla_range_wh_per_km", 145)))
        added_vehicle_wh = added_wall_wh * efficiency
        added_km = added_vehicle_wh / wh_per_km
        max_range = float(g_tesla_range_cache.get("max_range_km") or 397.0)
        charge_limit = float(g_tesla_range_cache.get("charge_limit_percent") or 100.0)
        target_range = max_range * (charge_limit / 100.0)
        range_km = min(target_range, base_range + added_km)
        soc_percent = max(0.0, min(100.0, (range_km / max_range) * 100.0)) if max_range > 0 else None
        return {
            "range_km": range_km,
            "soc_percent": soc_percent,
            "added_wall_wh": added_wall_wh,
            "added_vehicle_wh": added_vehicle_wh,
            "added_km": added_km,
            "efficiency": efficiency,
            "wh_per_km": wh_per_km,
        }
    except Exception as e:
        add_debug("Tesla range wall estimate compute failed", str(e), throttle_sec=300)
        return None


def finalize_tesla_wall_range_estimate(now=None, reason="session_end"):
    """充電セッション終了時にWall推定分をlast-known cacheへ反映して保存する。"""
    global g_tesla_range_cache
    if not g_tesla_range_cache.get("wall_estimate_active"):
        return False
    now = now or datetime.now(JST)
    est = _compute_wall_estimated_range(build_wall_connector_payload())
    if est:
        max_range = _safe_float(g_tesla_range_cache.get("max_range_km"), 397.0) or 397.0
        charge_limit = _safe_float(g_tesla_range_cache.get("charge_limit_percent"), 100.0) or 100.0
        target_range = max_range * (charge_limit / 100.0)
        range_km = est["range_km"]
        soc = est["soc_percent"]
        g_tesla_range_cache.update({
            "soc_percent": round(soc, 2) if soc is not None else None,
            "range_km": round(range_km, 3),
            "target_range_km": round(target_range, 3),
            "missing_km": round(max(0.0, target_range - range_km), 3),
            "source": "fleet_cached_plus_wall_estimate",
            "last_update": now.strftime('%Y-%m-%d %H:%M:%S'),
            "is_estimated": True,
            "wall_estimate_added_wh": round(est.get("added_vehicle_wh", 0.0), 1),
            "wall_estimate_added_km": round(est.get("added_km", 0.0), 2),
            "wall_estimate_finalized_reason": reason,
        })

    g_tesla_range_cache.update({
        "wall_estimate_active": False,
        "wall_estimate_started_at": None,
        "wall_estimate_start_session_wh": None,
        "wall_estimate_base_range_km": None,
        "wall_estimate_base_soc_percent": None,
    })
    save_tesla_range_cache()
    return True


def build_tesla_range_dataset(wall_payload=None):
    """
    /api/data 用のTesla SOC/rangeデータセット。

    方針:
      - Wall ConnectorにはSOC/kmがないので、SOC/km目的のFleet定期取得はしない。
      - Fleet分類時に得たbattery_levelをlast-knownとして使う。
      - 純粋なcharging中だけ、Wall Connector session_energy_wh増加で表示上のkmを推定加算する。
      - 取れていない値は0ではなくnullにする。
    """
    wall_payload = wall_payload or {}
    now = datetime.now(JST)
    load_tesla_range_cache()

    tm_soc = _safe_float(tesla_state.get("soc"), None)
    tm_range = _safe_float(tesla_state.get("range"), None)
    tm_max_range = _safe_float(tesla_state.get("max_range"), 397.0) or 397.0
    tm_charge_limit = _safe_float(tesla_state.get("charge_limit"), 100.0) or 100.0

    if (
        settings.get("tesla_range_use_teslamate_live", False)
        and tm_soc is not None and tm_soc > 0
        and tm_range is not None and tm_range > 0
    ):
        max_range = tm_max_range if tm_max_range > 0 else 397.0
        charge_limit = tm_charge_limit if tm_charge_limit > 0 else 100.0
        target_range = max_range * (charge_limit / 100.0)
        missing = max(0.0, target_range - tm_range)
        return {
            "soc_percent": round(tm_soc, 1),
            "range_km": round(tm_range, 1),
            "max_range_km": round(max_range, 1),
            "charge_limit_percent": round(charge_limit, 1),
            "target_range_km": round(target_range, 1),
            "missing_km": round(missing, 1),
            "data_source": "tesla_state",
            "source": "tesla_state",
            "last_update": tesla_state.get("last_update") or state.get("last_update") or now.strftime('%Y-%m-%d %H:%M:%S'),
            "age_sec": 0,
            "is_stale": False,
            "is_estimated": False,
            "wall_estimate_active": False,
            "note": "Live TeslaMate/tesla_state range data.",
        }

    cache = dict(g_tesla_range_cache)
    range_km = _safe_float(cache.get("range_km"), None)
    soc = _safe_float(cache.get("soc_percent"), None)
    max_range = _safe_float(cache.get("max_range_km"), 397.0) or 397.0
    charge_limit = _safe_float(cache.get("charge_limit_percent"), 100.0) or 100.0
    target_range = max_range * (charge_limit / 100.0)
    source = cache.get("source") or "unknown"
    is_estimated = bool(cache.get("is_estimated", True))

    # Heal old cache values created before Fleet battery_range miles->km handling.
    # If range_km is almost identical to battery_range_mi, it means miles were saved as km.
    cache_battery_range_km = _safe_float(cache.get("battery_range_km"), None)
    cache_battery_range_mi = _safe_float(cache.get("battery_range_mi"), None)
    if range_km is not None:
        if cache_battery_range_km is not None and abs(float(range_km) - float(cache_battery_range_km)) > 1.0:
            if cache_battery_range_mi is not None and abs(float(range_km) - float(cache_battery_range_mi)) < 1.0:
                range_km = cache_battery_range_km
                source = "fleet_cached_range_cache_healed_mi_to_km"
                is_estimated = True
        elif cache_battery_range_km is None and cache_battery_range_mi is not None and abs(float(range_km) - float(cache_battery_range_mi)) < 1.0:
            range_km = cache_battery_range_mi * MILES_TO_KM
            source = "fleet_cached_range_cache_healed_mi_to_km"
            is_estimated = True

    wall_est = None
    if ev_wall_state.get("load_type") == "charging" and cache.get("wall_estimate_active"):
        wall_est = _compute_wall_estimated_range(wall_payload)
        if wall_est:
            range_km = wall_est.get("range_km")
            soc = wall_est.get("soc_percent")
            source = "fleet_cached_plus_wall_live_estimate"
            is_estimated = True

    missing = max(0.0, target_range - range_km) if range_km is not None else None
    last_update = cache.get("last_update")
    last_dt = _parse_jst_time(last_update)
    age_sec = None
    if last_dt:
        try:
            age_sec = max(0, int((now - last_dt).total_seconds()))
        except Exception:
            age_sec = None

    stale_hours = float(settings.get("tesla_range_stale_hours", 24))
    is_stale = bool(age_sec is None or age_sec > stale_hours * 3600)

    return {
        "soc_percent": round(soc, 1) if soc is not None else None,
        "range_km": round(range_km, 1) if range_km is not None else None,
        "max_range_km": round(max_range, 1),
        "charge_limit_percent": round(charge_limit, 1),
        "target_range_km": round(target_range, 1),
        "missing_km": round(missing, 1) if missing is not None else None,
        "data_source": source,
        "source": source,
        "last_update": last_update,
        "age_sec": age_sec,
        "is_stale": is_stale,
        "is_estimated": is_estimated,
        "fleet_classification": cache.get("fleet_classification"),
        "usable_soc_percent": cache.get("usable_soc_percent"),
        "battery_range_mi": cache.get("battery_range_mi"),
        "battery_range_km": cache.get("battery_range_km"),
        "ideal_battery_range_mi": cache.get("ideal_battery_range_mi"),
        "ideal_battery_range_km": cache.get("ideal_battery_range_km"),
        "est_battery_range_mi": cache.get("est_battery_range_mi"),
        "est_battery_range_km": cache.get("est_battery_range_km"),
        "remaining_kwh": cache.get("remaining_kwh"),
        "remaining_kwh_estimated": cache.get("remaining_kwh_estimated"),
        "remaining_kwh_is_estimated": cache.get("remaining_kwh_is_estimated", True),
        "wall_estimate_active": bool(cache.get("wall_estimate_active")),
        "wall_estimate_started_at": cache.get("wall_estimate_started_at"),
        "wall_estimate_added_wh": round(wall_est.get("added_vehicle_wh", 0.0), 1) if wall_est else cache.get("wall_estimate_added_wh"),
        "wall_estimate_added_km": round(wall_est.get("added_km", 0.0), 2) if wall_est else cache.get("wall_estimate_added_km"),
        "note": "Last-known Fleet SOC/range. Fleet range is miles converted to km; charging adds Wall Connector Wh estimate.",
    }


def is_fleet_day_poll_window(now=None):
    """solar-assist windowのTesla/Solar計画用にFleet SOC/kmを更新する時間帯か。"""
    now = now or datetime.now(JST)
    # Prefer an explicit planning window; otherwise fall back to the solar-assist window.
    explicit_window = parse_contract_window("solar_planning_poll_window")
    if explicit_window:
        start, end = explicit_window
    else:
        start_raw = str(settings.get("fleet_day_poll_start", "") or "").strip()
        end_raw = str(settings.get("fleet_day_poll_end", "") or "").strip()
        if start_raw and end_raw:
            start = parse_hhmm_to_minutes(start_raw)
            end = parse_hhmm_to_minutes(end_raw)
        else:
            fallback = parse_contract_window("solar_assist_charge_window")
            if not fallback:
                return False
            start, end = fallback
    minutes = now.hour * 60 + now.minute
    return is_minutes_in_window(minutes, start, end)


def request_fleet_state_from_post(reason="event"):
    """
    POST側Fleet API代理エンドポイントを1回呼ぶ共通関数。

    重要:
      - この関数自体はTeslaMateを使わない。
      - 成功時はSOC/km last-known cacheを更新する。
      - 計画用ポーリングではev_wall_stateの分類キャッシュを汚さない。
    """
    now = datetime.now(JST)
    disabled_until = ev_wall_state.get("fleet_disabled_until")
    if disabled_until and now < disabled_until:
        return None

    endpoint = str(settings.get("fleet_state_endpoint", post_control_url("/api/tesla/fleet_state")))
    try:
        r = requests.get(endpoint, timeout=50)
        try:
            data = r.json()
        except Exception:
            data = {"status": "error", "stage": "json_parse", "body": r.text[:500]}

        if not (200 <= r.status_code < 300) or data.get("status") != "success":
            backoff = int(settings.get("fleet_error_backoff_sec", 1800))
            ev_wall_state["fleet_disabled_until"] = now + timedelta(seconds=backoff)
            ev_wall_state["last_error"] = f"fleet_state HTTP {r.status_code}: {str(data)[:300]}"
            add_log("Fleet取得失敗", f"{reason}: {ev_wall_state['last_error']} / {backoff}秒抑止", "warning")
            return None

        ev_wall_state["fleet_disabled_until"] = None
        update_tesla_range_cache_from_fleet(data, now=now)
        return data
    except Exception as e:
        backoff = int(settings.get("fleet_error_backoff_sec", 1800))
        ev_wall_state["fleet_disabled_until"] = now + timedelta(seconds=backoff)
        ev_wall_state["last_error"] = f"fleet_state exception: {e}"
        add_log("Fleet取得失敗", f"{reason}: {e} / {backoff}秒抑止", "warning")
        return None


def maybe_poll_fleet_for_day_planning(now=None):
    """
    solar-assist window制御計画用Fleetポーリング。

    方針:
      - configured planning windowだけ一定間隔で取得する。
      - Wall Connector未接続ならスキップ可能。
      - 分類キャッシュは更新せず、SOC/km cacheだけ更新する。
      - primaryprimary charge windowはここでは取らない。充電開始/完了イベントだけにする。
    """
    global g_last_fleet_planning_poll_time, g_last_fleet_planning_poll_reason
    if not settings.get("fleet_day_poll_enabled", True):
        return False
    now = now or datetime.now(JST)
    if not is_fleet_day_poll_window(now):
        return False
    if settings.get("fleet_poll_only_when_wall_connected", True) and not ev_wall_state.get("vehicle_connected", False):
        return False

    interval = max(60, int(settings.get("fleet_day_poll_interval_sec", 300)))
    if g_last_fleet_planning_poll_time is not None:
        if (now - g_last_fleet_planning_poll_time).total_seconds() < interval:
            return False

    data = request_fleet_state_from_post(reason="day_planning_poll")
    if data:
        g_last_fleet_planning_poll_time = now
        g_last_fleet_planning_poll_reason = "day_planning_poll"
        add_debug(
            "Fleet計画用取得",
            "昼のTesla/Solar計画用にSOC/kmを更新しました",
            {"classification": data.get("classification"), "ts": data.get("ts")},
            throttle_sec=60,
        )
        return True
    return False


def fetch_fleet_state_for_charging_stop(reason="charging_stop"):
    """充電完了/停止時にFleetを1回取得して、Wall推定ではなく実SOCでlast-knownを確定する。"""
    if not settings.get("fleet_fetch_on_charging_stop", True):
        return None
    return request_fleet_state_from_post(reason=reason)

def reset_wall_fleet_classification(reason="low_power"):
    """
    Wall電力がon閾値未満に落ちた時、Fleet分類キャッシュをセッション内で破棄する。
    次に200W以上へ再上昇したらFleet APIを再取得して、充電/空調を分類し直す。
    """
    old_type = ev_wall_state.get("load_type", "none")
    if old_type == "charging":
        finalize_tesla_wall_range_estimate(reason=f"reset_{reason}")
    ev_wall_state["load_type"] = "none"
    ev_wall_state["fleet_checked_for_session"] = False
    ev_wall_state["fleet_data"] = None
    ev_wall_state["fleet_last_fetch"] = None
    notify_post_tesla_charging_state(False, reason=f"wall_{reason}_{old_type}")
    return old_type


def notify_post_tesla_charging_state(is_charging, reason=""):
    """
    A案: 既存POST互換の update_tesla_state だけ使う。
    charging系のときだけ True、climate_only は False。
    """
    global g_last_notified_tesla_charging
    is_charging = bool(is_charging)
    if is_charging == g_last_notified_tesla_charging:
        return False
    g_last_notified_tesla_charging = is_charging
    try:
        requests.post(
            post_control_url("/api/command"),
            json={"target": "system", "action": "update_tesla_state", "value": is_charging},
            timeout=2,
        )
        add_debug("POST Tesla状態同期", f"charging={is_charging} / {reason}", {"reason": reason})
        return True
    except Exception as e:
        add_log("POST Tesla状態同期失敗", f"charging={is_charging}: {e}", "warning")
        return False


def fetch_fleet_state_once_for_ev_load(force=False, reason="ev_load_classification"):
    """POST側のFleet API代理エンドポイントを、EV負荷セッション分類時に呼ぶ。"""
    now = datetime.now(JST)
    if not force:
        last_fetch = ev_wall_state.get("fleet_last_fetch")
        ttl = int(settings.get("fleet_classify_ttl_sec", 900))
        if last_fetch and (now - last_fetch).total_seconds() < ttl and ev_wall_state.get("fleet_data"):
            return ev_wall_state.get("fleet_data")

    data = request_fleet_state_from_post(reason=reason)
    if not data:
        return None

    # 分類用に取得した時だけ、Wall Connectorセッションの分類キャッシュへ反映する。
    ev_wall_state["fleet_last_fetch"] = now
    ev_wall_state["fleet_data"] = data
    return data

def classify_wall_ev_load_with_fleet():
    """Wall Connector負荷に対して、Fleet分類結果を反映する。"""
    data = fetch_fleet_state_once_for_ev_load()
    ev_wall_state["fleet_checked_for_session"] = True
    if not data:
        ev_wall_state["load_type"] = "unknown_ev_load"
        notify_post_tesla_charging_state(False, reason="fleet_failed_unknown_ev_load")
        return "unknown_ev_load"

    cls = str(data.get("classification") or "unknown_ev_load")
    if cls not in ["charging", "charging_with_climate", "climate_only", "none"]:
        cls = "unknown_ev_load"
    # Wall Connectorで実電力が出ているのにFleet分類noneならunknown扱いにする。
    if cls == "none" and ev_wall_state.get("load_active"):
        cls = "unknown_ev_load"

    ev_wall_state["load_type"] = cls
    if cls == "charging":
        start_tesla_wall_range_estimate(now=datetime.now(JST))
    else:
        finalize_tesla_wall_range_estimate(now=datetime.now(JST), reason=f"classified_{cls}")
    notify_post_tesla_charging_state(fleet_classification_is_charging(cls), reason=f"wall_fleet_{cls}")
    add_log(
        "EV負荷分類",
        f"Wall {ev_wall_state.get('power_w', 0):.0f}W / Fleet={cls}",
        "info" if cls in ["charging", "climate_only", "charging_with_climate"] else "warning",
    )
    return cls


def poll_wall_connector_once():
    """Wall Connector local APIを1回取得し、EV負荷セッションと分類を更新する。"""
    if not settings.get("wall_connector_enabled", True):
        ev_wall_state["enabled"] = False
        return

    now = datetime.now(JST)
    try:
        r = requests.get(wall_connector_url(), timeout=3)
        r.raise_for_status()
        data = r.json()

        current_a = float(data.get("vehicle_current_a") or 0)
        grid_v = float(data.get("grid_v") or 0)
        power_w = max(0.0, current_a * grid_v)
        contactor_closed = bool(data.get("contactor_closed"))

        ev_wall_state.update({
            "enabled": True,
            "api_ok": True,
            "vehicle_connected": bool(data.get("vehicle_connected")),
            "contactor_closed": contactor_closed,
            "current_a": current_a,
            "grid_v": grid_v,
            "power_w": power_w,
            "session_energy_wh": float(data.get("session_energy_wh") or 0),
            "evse_state": data.get("evse_state"),
            "last_update": now,
            "last_error": "",
        })

        # 昼のsolar assist window制御計画用SOC/km更新。分類キャッシュは汚さない。
        maybe_poll_fleet_for_day_planning(now)

        high_power_now = wall_ev_power_active(power_w)
        low_power_present = wall_ev_low_power_present(power_w, current_a, contactor_closed)
        activity_present = high_power_now or low_power_present

        if activity_present:
            ev_wall_state["last_activity_time"] = now
            if high_power_now:
                ev_wall_state["last_power_time"] = now
                ev_wall_state["low_power_since"] = None
            else:
                if ev_wall_state.get("low_power_since") is None:
                    ev_wall_state["low_power_since"] = now
                # 200W未満に落ちたら、POST側への「充電中」通知だけは即False。
                # ただし低出力の空調/温調はREAD側ではEV負荷として追跡し続ける。
                notify_post_tesla_charging_state(False, reason="wall_low_power_present_not_charging")

            if not ev_wall_state.get("load_active"):
                ev_wall_state["load_active"] = True
                ev_wall_state["load_since"] = now
                ev_wall_state["load_type"] = "detecting" if high_power_now else "detecting_low_power"
                ev_wall_state["fleet_checked_for_session"] = False

            reclass_sec = int(settings.get("wall_low_power_reclassify_sec", 10))
            low_since = ev_wall_state.get("low_power_since") or now

            if high_power_now:
                if not ev_wall_state.get("fleet_checked_for_session"):
                    classify_wall_ev_load_with_fleet()
            else:
                # 充電→停止→低出力空調のような場合は、古いcharging分類を短時間で破棄して
                # 低出力のままFleetを再取得する。200W超えを待たない。
                old_type = ev_wall_state.get("load_type", "none")
                if (
                    ev_wall_state.get("fleet_checked_for_session")
                    and fleet_classification_is_charging(old_type)
                    and (now - low_since).total_seconds() >= reclass_sec
                ):
                    old_type = reset_wall_fleet_classification(reason="low_power_reclassify")
                    add_log(
                        "EV負荷分類リセット",
                        f"Wall {power_w:.0f}W の低出力が{reclass_sec}秒以上続いたため、Fleet分類 {old_type} を破棄して再分類します",
                        "info",
                    )

                if not ev_wall_state.get("fleet_checked_for_session") and (now - low_since).total_seconds() >= reclass_sec:
                    classify_wall_ev_load_with_fleet()

            return

        # ここからはWall Connector経由のEV負荷が実質ない状態。
        # 充電停止後に contactor=false / 0W がしばらく続いた場合は、
        # EV負荷セッション自体は早めに切る。
        # 理由: load_active を180秒ラッチしたままだと、直後のエアコン開始時に
        # 直前のFleet分類(charging)を引きずって再分類されないため。
        if ev_wall_state.get("load_active"):
            if ev_wall_state.get("low_power_since") is None:
                ev_wall_state["low_power_since"] = now

            notify_post_tesla_charging_state(False, reason="wall_activity_absent")

            no_activity_since = ev_wall_state.get("low_power_since") or now
            no_activity_reset_sec = int(settings.get("wall_no_activity_reset_sec", settings.get("wall_low_power_reclassify_sec", 10)))
            if (now - no_activity_since).total_seconds() >= no_activity_reset_sec:
                old_type = ev_wall_state.get("load_type", "none")
                if old_type == "charging":
                    finalize_tesla_wall_range_estimate(now=now, reason="wall_no_activity_reset")
                    fetch_fleet_state_for_charging_stop(reason="wall_no_activity_reset_charging_stop")
                ev_wall_state["load_active"] = False
                ev_wall_state["load_type"] = "none"
                ev_wall_state["load_since"] = None
                ev_wall_state["low_power_since"] = None
                ev_wall_state["fleet_checked_for_session"] = False
                ev_wall_state["fleet_data"] = None
                ev_wall_state["fleet_last_fetch"] = None
                notify_post_tesla_charging_state(False, reason=f"wall_no_activity_reset_{old_type}")
                add_log("EV負荷分類リセット", f"Wall Connector負荷が{no_activity_reset_sec}秒以上0Wのため、Fleet分類 {old_type} を破棄しました", "info")
                return

            # 念のため、長時間無負荷が続いた場合も終了扱いにする。
            # 通常は上のno_activity_resetで先に抜ける。
            last_activity = ev_wall_state.get("last_activity_time") or ev_wall_state.get("last_power_time") or now
            end_sec = int(settings.get("ev_session_end_sec", 180))
            if (now - last_activity).total_seconds() >= end_sec:
                old_type = ev_wall_state.get("load_type", "none")
                if old_type == "charging":
                    finalize_tesla_wall_range_estimate(now=now, reason="wall_session_end")
                    fetch_fleet_state_for_charging_stop(reason="wall_session_end_charging_stop")
                ev_wall_state["load_active"] = False
                ev_wall_state["load_type"] = "none"
                ev_wall_state["load_since"] = None
                ev_wall_state["low_power_since"] = None
                ev_wall_state["fleet_checked_for_session"] = False
                ev_wall_state["fleet_data"] = None
                ev_wall_state["fleet_last_fetch"] = None
                notify_post_tesla_charging_state(False, reason=f"wall_session_end_{old_type}")
                add_log("EV負荷終了", f"Wall Connector負荷が{end_sec}秒以上検出されませんでした", "info")
    except Exception as e:
        ev_wall_state["api_ok"] = False
        ev_wall_state["last_error"] = str(e)
        ev_wall_state["last_update"] = now
        add_debug("Wall Connector取得失敗", str(e), throttle_sec=60)


def wall_connector_worker():
    time.sleep(8)
    while True:
        try:
            poll_wall_connector_once()
        except Exception as e:
            add_log("Wall Connector監視エラー", str(e), "warning")
        time.sleep(max(2, int(settings.get("wall_poll_interval_sec", 5))))


def build_wall_connector_payload():
    payload = dict(ev_wall_state)
    for key in ["load_since", "last_power_time", "last_activity_time", "low_power_since", "last_update", "fleet_last_fetch", "fleet_disabled_until"]:
        v = payload.get(key)
        if isinstance(v, datetime):
            payload[key] = v.strftime("%Y-%m-%d %H:%M:%S")
    payload["post_update_charging"] = wall_connector_should_notify_charging()
    return payload



def battery_command_policy_override(action):
    """
    蓄電池コマンド最終調停。

    Battery First中は、どの旧ロジックから auto/standby が飛んできても最終出口で拒否する。
    これにより auto -> standby -> charge のような制御喧嘩を止める。

    戻り値:
      (allowed: bool, effective_action: str, reason: str)
    """
    try:
        action = str(action).lower()
        now = datetime.now(JST)

        if not is_solar_assist_charge_window(now):
            return True, action, ""

        # 制御に必要な現在値を安全に取得
        total = estimate_total_home_consumption_w()
        try:
            is_tesla_home_charging(total)
        except Exception:
            pass
        pure = get_pure_home_consumption_w(total)

        required, reason, target_soc, afternoon = is_day_battery_first_required(now, pure)
        if not required:
            return True, action, ""

        # Battery First未達中はcharge以外を許可しない
        if action != "charge":
            return False, action, f"Battery First中のため {action} を拒否し、chargeを維持します。Target {target_soc}% / {reason}"

        return True, action, f"Battery First中のcharge許可。Target {target_soc}% / {reason}"

    except Exception as e:
        # 調停エラー時は安全側。コマンド自体は許可して制御停止を避ける。
        return True, str(action).lower(), f"battery_command_policy_override error: {e}"


def request_battery_command(action, force=False):
    """
    蓄電池モード変更コマンドを送る。

    重要:
      以前は g_ctrl_status["status"] だけを見ていたため、
      制御サーバーが mode/intended_mode で返す環境では
      「すでにcharge中」なのにchargeを連打するバグがあった。

    対策:
      - status/mode/intended_mode を正規化して比較
      - 同一コマンドは一定時間再送しない
      - 成功後は status/mode/intended_mode をローカルで即時更新
    """
    global g_ctrl_status, g_last_battery_command_action, g_last_battery_command_time

    action = str(action).lower()
    allowed, effective_action, override_reason = battery_command_policy_override(action)
    if not allowed:
        # 旧ロジックがBattery First中にauto/standbyを投げても、ここで最終的に止める。
        # ログは多すぎないように直近同一actionは抑制する。
        try:
            global g_last_battery_policy_block_time, g_last_battery_policy_block_key
        except Exception:
            pass
        if "g_last_battery_policy_block_time" not in globals():
            g_last_battery_policy_block_time = None
        if "g_last_battery_policy_block_key" not in globals():
            g_last_battery_policy_block_key = ""

        now_for_block = datetime.now(JST)
        block_key = f"{action}:{override_reason}"
        if (
            g_last_battery_policy_block_time is None
            or block_key != g_last_battery_policy_block_key
            or (now_for_block - g_last_battery_policy_block_time).total_seconds() >= 60
        ):
            add_log("蓄電池コマンド抑止", override_reason, "warning")
            g_last_battery_policy_block_time = now_for_block
            g_last_battery_policy_block_key = block_key
        return False
    action = effective_action

    now = datetime.now(JST)
    current_mode = get_effective_battery_mode(g_ctrl_status)

    # force=Trueでも同一コマンドの超短時間連打は防ぐ。
    # POST側の/status反映遅延中にBattery Firstが毎周期chargeを投げるのを防止する。
    force_duplicate_guard_sec = 5
    if (
        g_last_battery_command_action == action
        and g_last_battery_command_time is not None
        and (now - g_last_battery_command_time).total_seconds() < force_duplicate_guard_sec
    ):
        return False

    # すでに同じモードなら送らない
    if not force and current_mode == action:
        return False

    # 直近に同じコマンドを送っているなら送らない
    if (
        not force
        and g_last_battery_command_action == action
        and g_last_battery_command_time is not None
        and (now - g_last_battery_command_time).total_seconds() < g_battery_command_min_interval_sec
    ):
        return False

    try:
        r = requests.post(
            post_control_url("/api/command"),
            json={"action": action},
            timeout=2
        )

        if 200 <= r.status_code < 300:
            g_last_battery_command_action = action
            g_last_battery_command_time = now

            # ローカル状態を即時更新して、次周期で再送しないようにする
            g_ctrl_status["status"] = action
            g_ctrl_status["mode"] = action
            g_ctrl_status["intended_mode"] = action
            return True

        add_log("蓄電池コマンド失敗", f"{action} HTTP {r.status_code}", "warning")
        return False

    except Exception as e:
        add_log("蓄電池コマンド失敗", f"{action}: {e}", "warning")
        return False


def get_tesla_missing_km():
    """Fleet last-known + Wall推定のrangeから、charge_limit到達までの不足kmを見積もる。"""
    try:
        ds = build_tesla_range_dataset(build_wall_connector_payload())
        missing = _safe_float(ds.get("missing_km"), None)
        if missing is not None:
            return max(0.0, missing)
    except Exception:
        pass
    try:
        soc = float(tesla_state.get("soc", 0) or 0)
        charge_limit = float(tesla_state.get("charge_limit", 100) or 100)
        if soc <= 0 or soc >= charge_limit:
            return 0.0
        max_range = float(tesla_state.get("max_range", 397.0) or 397.0)
        current_range = float(tesla_state.get("range", 0) or 0)
        target_range = max_range * (charge_limit / 100.0)
        return max(0.0, target_range - current_range)
    except Exception:
        return 0.0


def can_send_tesla_start(min_missing_km=None):
    global g_last_tesla_start_time, g_tesla_suspended
    now = datetime.now(JST)
    if not can_control_tesla_now(): return False
    if g_tesla_suspended: return False
    if not tesla_state.get("plugged_in", True): return False
    if tesla_state["charging_state"] in ["Charging", "Starting"]: return False
    if tesla_state["soc"] >= tesla_state["charge_limit"]: return False

    # Teslaは、charge_limit近辺や不足が小さい状態ではstart要求を受けても
    # 実際には充電を開始しないことがある。無駄な開始連打を避ける。
    if min_missing_km is None:
        min_missing_km = float(settings.get("tesla_start_min_missing_km", 8))
    if get_tesla_missing_km() < float(min_missing_km): return False

    # Teslaは充電上限近辺だと、start要求を受けても開始しないことがある。
    # 航続距離が設定値より高い場合は、無駄な開始要求を送らない。
    try:
        start_max_range_km = float(settings.get("tesla_start_max_range_km", 388.0) or 0)
        if start_max_range_km > 0 and float(tesla_state.get("range", 0) or 0) > start_max_range_km:
            return False
    except Exception:
        pass

    if g_last_tesla_start_time and (now - g_last_tesla_start_time).total_seconds() < 15: return False
    return True



def estimate_total_home_consumption_w():
    """HEMSから見た家全体の負荷推定W。"""
    return max(0.0, float(state.get("solar", 0)) + float(state.get("grid", 0)) - float(state.get("battery", 0)))


def normalize_geofence_name(name):
    return str(name or "").strip().lower()



def distance_m_between(lat1, lon1, lat2, lon2):
    """2点間の距離[m]を概算する。TeslaMate location fallback用。"""
    try:
        from math import radians, sin, cos, sqrt, atan2
        r = 6371000.0
        p1 = radians(float(lat1))
        p2 = radians(float(lat2))
        dp = radians(float(lat2) - float(lat1))
        dl = radians(float(lon2) - float(lon1))
        a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return r * c
    except Exception:
        return None


def is_tesla_at_home_by_location_radius():
    """
    TeslaMate geofenceが空の場合のfallback。
    locationが自宅座標から一定半径内なら自宅扱い。
    電力一致ではなくGPS座標ベースなので、外部6kW普通充電の誤判定を避けやすい。
    """
    loc = tesla_state.get("location")
    if not isinstance(loc, dict):
        return None

    lat = loc.get("latitude")
    lon = loc.get("longitude")
    home_lat = settings.get("tesla_home_latitude")
    home_lon = settings.get("tesla_home_longitude")
    radius_m = float(settings.get("tesla_home_radius_m", 120))

    if lat is None or lon is None or home_lat is None or home_lon is None:
        return None

    dist = distance_m_between(lat, lon, home_lat, home_lon)
    tesla_state["home_distance_m"] = None if dist is None else round(dist, 1)

    if dist is None:
        return None

    return dist <= radius_m


def is_tesla_at_home_by_location():
    """
    Teslaが自宅にいると確定できるか。

    優先順位:
      1. is_home が明示 True/False
      2. geofence が Home系なら True
      3. geofence が Home以外なら False
      4. geofence が空で location が自宅半径内なら True
      5. location が自宅半径外なら False
      6. どちらも無ければ None

    電力一致では判定しない。
    """
    explicit = tesla_state.get("is_home", None)
    if explicit is True:
        return True
    if explicit is False:
        return False

    geofence = normalize_geofence_name(tesla_state.get("geofence", ""))
    home_names = [normalize_geofence_name(x) for x in settings.get("tesla_home_geofence_names", [])]

    if geofence:
        return geofence in home_names

    radius_home = is_tesla_at_home_by_location_radius()
    if radius_home is True:
        tesla_state["home_charge_reason"] = f"location_radius_home_{tesla_state.get('home_distance_m')}m"
        return True
    if radius_home is False:
        tesla_state["home_charge_reason"] = f"location_radius_not_home_{tesla_state.get('home_distance_m')}m"
        return False

    return None


def is_tesla_home_charging(total_consumption_w=None):
    """
    Teslaが「自宅で充電している」と見なせるか。

    重大方針:
      電力一致だけでは自宅判定しない。
      外部6kW普通充電器と自宅消費が偶然近い場合に誤制御するため。

    デフォルト:
      strict: geofence/Homeで自宅確定した場合のみTrue。
      meter_fallbackは明示設定時のみ使えるが、通常は無効。
    """
    try:
        # Wall ConnectorローカルAPIが充電負荷を示す場合、自宅充電と確定する。
        # TeslaMate MQTTを無効化しても、A制御・ブレーカー保護を継続できるようにする。
        if wall_connector_indicates_charging_load():
            tesla_state["home_charge_reason"] = "wall_connector_charging"
            tesla_state["home_charging"] = True
            return True

        if tesla_state.get("charging_state") not in ["Charging", "Starting"]:
            tesla_state["home_charge_reason"] = "not_charging"
            tesla_state["home_charging"] = False
            return False

        if not tesla_state.get("plugged_in", False):
            tesla_state["home_charge_reason"] = "not_plugged"
            tesla_state["home_charging"] = False
            return False

        location_home = is_tesla_at_home_by_location()
        detection_mode = settings.get("tesla_home_detection_mode", "strict")
        allow_meter_only = bool(settings.get("tesla_allow_meter_only_home_detection", False))

        charger_kw = float(tesla_state.get("charger_power", tesla_state.get("power", 0)) or 0)
        power_w = max(0.0, charger_kw * 1000.0)
        current_amps = max(
            int(float(tesla_state.get("request_current", 0) or 0)),
            int(float(tesla_state.get("actual_current", 0) or 0))
        )

        max_home_power_w = float(settings.get("tesla_home_charging_max_power_w", 12000))
        if power_w > max_home_power_w:
            tesla_state["home_charge_reason"] = f"power_too_high_{int(power_w)}W"
            tesla_state["home_charging"] = False
            return False

        if current_amps > int(sys_settings.get("MAX_CHARGE_AMPS", 32)) + 2:
            tesla_state["home_charge_reason"] = f"amps_too_high_{current_amps}A"
            tesla_state["home_charging"] = False
            return False

        if location_home is True:
            tesla_state["home_charge_reason"] = "location_home"
            tesla_state["home_charging"] = True
            return True

        if location_home is False:
            tesla_state["home_charge_reason"] = f"location_not_home_{tesla_state.get('geofence', '')}"
            tesla_state["home_charging"] = False
            return False

        # 位置不明。strictでは制御しない。
        if detection_mode == "strict" or not allow_meter_only:
            tesla_state["home_charge_reason"] = "location_unknown_fail_safe"
            tesla_state["home_charging"] = False
            return False

        # 明示的に meter_fallback を有効化した場合のみ。
        if total_consumption_w is None:
            total_consumption_w = estimate_total_home_consumption_w()

        min_power_w = float(settings.get("tesla_home_charging_min_power_w", 300))
        margin_w = float(settings.get("tesla_home_charging_meter_margin_w", 1200))

        if power_w < min_power_w:
            tesla_state["home_charge_reason"] = "location_unknown_low_power_no_control"
            tesla_state["home_charging"] = False
            return False

        if total_consumption_w + margin_w >= power_w:
            tesla_state["home_charge_reason"] = f"meter_fallback_match_load_{int(total_consumption_w)}W_power_{int(power_w)}W"
            tesla_state["home_charging"] = True
            return True

        tesla_state["home_charge_reason"] = f"meter_fallback_mismatch_load_{int(total_consumption_w)}W_power_{int(power_w)}W"
        tesla_state["home_charging"] = False
        return False

    except Exception as e:
        tesla_state["home_charge_reason"] = f"detect_error_{e}"
        tesla_state["home_charging"] = False
        return False


def can_control_tesla_now():
    """
    HEMSがTeslaへ start/stop/amps を送ってよいか。

    自宅にいることがTeslaMate geofenceで確定していない場合はFalse。
    停止中のstart指示でも、自宅確定が必要。
    """
    if not settings.get("tesla_control_only_when_home_charging", True):
        tesla_state["control_allowed"] = True
        return True

    # Wall Connectorは自宅固定IPなので、接続中なら自宅制御を許可する。
    # TeslaMate/geofenceが止まっていても、Wall基準で安全にA制御できる。
    if wall_connector_indicates_home_connected():
        tesla_state["home_charge_reason"] = "control_allowed_by_wall_connector"
        tesla_state["control_allowed"] = True
        return True

    location_home = is_tesla_at_home_by_location()
    if location_home is not True:
        tesla_state["home_charge_reason"] = "control_denied_not_confirmed_home"
        tesla_state["control_allowed"] = False
        return False

    tesla_state["control_allowed"] = True
    return True


def get_tesla_home_power_w():
    """自宅充電中と確定できる場合だけ、Tesla充電電力Wを返す。

    Wall ConnectorローカルAPIが取れている場合は、TeslaMate/Fleetのcharger_powerより
    Wall Connector実測Wを優先する。これにより、消費電力更新の遅れで純家庭負荷を
    過大/過小評価するのを防ぐ。
    """
    if not tesla_state.get("home_charging", False) and not wall_connector_indicates_tesla_load():
        return 0.0
    try:
        if settings.get("wall_connector_power_as_control_source", True):
            if ev_wall_state.get("api_ok") and ev_wall_state.get("vehicle_connected"):
                wall_w = max(0.0, float(ev_wall_state.get("power_w") or 0.0))
                if wall_w > 0:
                    return wall_w
    except Exception:
        pass
    return max(0.0, float(tesla_state.get("charger_power", tesla_state.get("power", 0)) or 0) * 1000.0)


def get_pure_home_consumption_w(total_consumption_w=None):
    """家全体負荷から、自宅充電中のTesla分だけを差し引いた家庭純消費W。"""
    if total_consumption_w is None:
        total_consumption_w = estimate_total_home_consumption_w()
    return max(0.0, float(total_consumption_w) - get_tesla_home_power_w())



def get_effective_day_target(now=None):
    """solar assist制御で使う実効Day Target。手動指定中は手動値を優先する。"""
    if now is None:
        now = datetime.now(JST)
    try:
        if g_manual_day_target_until and now < g_manual_day_target_until and g_manual_day_target_value is not None:
            return int(g_manual_day_target_value)
    except Exception:
        pass
    return int(forecast_data.get("predicted_soc_window_end", g_ctrl_status.get("day_threshold", 50)) or 50)


def forecast_kwh_between(start_hour, end_hour):
    """today_hourlyから指定時間帯の発電予測kWhを合計する。end_hourは未満。"""
    total = 0.0
    for h in forecast_data.get("today_hourly", []):
        try:
            hour = int(str(h.get("time", "00:00")).split(":")[0])
            if start_hour <= hour < end_hour:
                total += float(h.get("gen", 0) or 0)
        except Exception:
            pass
    return total


def estimate_afternoon_recovery(now, pure_consumption):
    """
    solar window end以降の太陽光で、夕方Targetまで蓄電池を回復できるかを推定する。
    この判定がTrueなら、solar assist windowに蓄電池へ無理なグリッド充電をしなくてもよい。
    """
    batt_max_kwh = float(sys_settings.get("BATT_MAX_WH", 12600)) / 1000.0
    target_soc = int(settings.get("afternoon_target_soc", 70))
    target_hour = int(settings.get("afternoon_target_hour", 17))
    target_hour = max(14, min(23, target_hour))

    solar_end_hour = int(get_window_end_hour_float("solar_assist_charge_window", now, fallback=now.hour))
    solar_end_hour = max(0, min(23, solar_end_hour))
    target_hour = max(solar_end_hour + 1, min(23, target_hour))
    afternoon_solar_kwh = forecast_kwh_between(solar_end_hour, target_hour)
    house_after_solar_window_kwh = max(0.0, (pure_consumption / 1000.0) * max(0, target_hour - solar_end_hour))
    battery_needed_kwh = max(0.0, (target_soc - state["soc"]) / 100.0 * batt_max_kwh)
    solar_for_battery_kwh = max(0.0, afternoon_solar_kwh - house_after_solar_window_kwh)
    deficit_kwh = max(0.0, battery_needed_kwh - solar_for_battery_kwh)

    # solar window endまでに最低限積んでおきたいSOC。solar window後だけでは足りない分を現在SOCに足す。
    required_pre_solar_window_soc = min(100, round(state["soc"] + (deficit_kwh / batt_max_kwh * 100.0))) if batt_max_kwh > 0 else target_soc

    return {
        "target_soc": target_soc,
        "target_hour": target_hour,
        "afternoon_solar_kwh": round(afternoon_solar_kwh, 2),
        "house_after_solar_window_kwh": round(house_after_solar_window_kwh, 2),
        "battery_needed_kwh": round(battery_needed_kwh, 2),
        "solar_for_battery_kwh": round(solar_for_battery_kwh, 2),
        "deficit_kwh": round(deficit_kwh, 2),
        "required_pre_solar_window_soc": int(required_pre_solar_window_soc),
        "can_recover_after_solar_window": deficit_kwh <= 0.1,
    }


def apply_tesla_amp_target(target_amps, current_amps, now, reason=""):
    """
    Tesla電流変更を落ち着かせて適用する。

    目的:
      SOC予測や発電予測の小さな揺れで、
      7A→9A→7A→10A のように頻繁に変更されるのを防ぐ。

    ルール:
      - 1A程度の差は無視する
      - 上げる方向はゆっくり
      - 下げる方向は安全側なので少し早め
      - 同じ目標Aが一定時間維持された場合のみ反映
    """
    global g_last_increase_time, g_last_decrease_time
    global g_last_tesla_target_amps, g_last_tesla_target_since
    global g_last_tesla_commanded_amps

    max_amps = int(sys_settings.get("MAX_CHARGE_AMPS", 32))
    min_amps = int(settings.get("solar_priority_min_amps", 5))

    increase_cd = int(settings.get("tesla_amp_increase_cooldown_sec", 120))
    decrease_cd = int(settings.get("tesla_amp_decrease_cooldown_sec", 60))
    deadband_a = max(1, int(settings.get("tesla_amp_change_deadband_a", 2)))
    hold_sec = max(0, int(settings.get("tesla_amp_target_hold_sec", 30)))

    target_amps = max(min_amps, min(max_amps, int(target_amps)))
    current_amps = int(current_amps or 0)
    effective_current = int(g_last_tesla_commanded_amps or current_amps or 0)

    # 変化幅が小さい場合は無視。予測の小刻みな揺れをTeslaへ伝播させない。
    if abs(target_amps - effective_current) < deadband_a:
        return False

    # 同じtargetが一定時間続くまで待つ。
    # ただし安全側の大幅降下はholdを短く扱う。
    if g_last_tesla_target_amps != target_amps:
        g_last_tesla_target_amps = target_amps
        g_last_tesla_target_since = now
        return False

    if g_last_tesla_target_since and (now - g_last_tesla_target_since).total_seconds() < hold_sec:
        return False

    if target_amps > effective_current:
        if g_last_increase_time and (now - g_last_increase_time).total_seconds() < increase_cd:
            return False
        if request_tesla_amps_change(target_amps):
            g_last_increase_time = now
            if reason:
                add_log("Tesla電流増加", f"{effective_current}A ➔ {target_amps}A / {reason}", "info")
            return True

    elif target_amps < effective_current:
        if g_last_decrease_time and (now - g_last_decrease_time).total_seconds() < decrease_cd:
            return False
        if request_tesla_amps_change(target_amps):
            g_last_decrease_time = now
            if reason:
                add_log("Tesla電流低下", f"{effective_current}A ➔ {target_amps}A / {reason}", "warning")
            return True

    return False



def should_allow_battery_auto_absorb_solar():
    """
    solar assist windowにTesla充電が終わっている/していない場合、
    蓄電池が100%未満ならStandby保持ではなくAutoで太陽光を吸わせる。

    Standbyが必要なのは主にTesla充電中に蓄電池からTeslaへ流れるのを防ぐため。
    Teslaが止まっていれば、Standbyのままだと太陽光が売電されてしまう。
    """
    if tesla_state.get("home_charging", False):
        return False
    return state.get("soc", 0) < 100


def check_grid_safety_limit():
    """制御ループ入口。WebSocket更新と周期workerの同時実行を防ぐ。"""
    global g_last_control_skip_log_time
    if not g_control_lock.acquire(blocking=False):
        now = datetime.now(JST)
        if g_last_control_skip_log_time is None or (now - g_last_control_skip_log_time).total_seconds() >= 60:
            add_debug("制御ループ多重起動抑止", "前回の制御周期がまだ実行中のため、この周期をスキップしました", throttle_sec=60)
            g_last_control_skip_log_time = now
        return
    try:
        return _check_grid_safety_limit_impl()
    finally:
        g_control_lock.release()


def _check_grid_safety_limit_impl():
    # --- core local defaults ---
    # ここで制御ループ全体で使う基本値を必ず初期化する。
    # 後段のDay Priority / Flow / Tesla制御で未定義参照になるのを防ぐ。
    now = datetime.now(JST)
    total_consumption = estimate_total_home_consumption_w()
    try:
        is_tesla_home_charging(total_consumption)
    except Exception:
        pass
    pure_consumption = get_pure_home_consumption_w(total_consumption)
    current_amps = get_tesla_control_current_amps()
    is_charging = tesla_amp_control_active(total_consumption)

    # Tesla残量不足量も制御ループ全体で使うため先に初期化する。
    try:
        tesla_target_range_km = float(tesla_state.get("max_range", 397.0)) * (float(tesla_state.get("charge_limit", 100)) / 100.0)
        tesla_missing_km = max(0.0, tesla_target_range_km - float(tesla_state.get("range", 0) or 0))
        tesla_rem_kwh = tesla_missing_km * 0.145
    except Exception:
        tesla_target_range_km = 0.0
        tesla_missing_km = 0.0
        tesla_rem_kwh = 0.0

    global g_warning_start_time, g_last_decrease_time, g_last_increase_time, g_low_surplus_start_time
    global g_last_notified_tesla_charging, g_last_tesla_start_time, g_last_skip_log_time
    global g_tesla_start_retries, g_tesla_suspended, g_read_only_mode, g_last_reset_hour
    global g_ctrl_status, g_last_day_standby_time, g_day_standby_by_auto
    global g_manual_day_target_until, g_manual_day_target_value
    global g_last_tesla_idle_reset_log_time, g_last_tesla_critical_drop_time

    if g_read_only_mode:
        return

    now = datetime.now(JST)
    current_grid = state["grid"]
    is_tesla_charging = (tesla_state["charging_state"] in ["Charging", "Starting"]) or wall_connector_indicates_charging_load()
    current_amps = get_tesla_control_current_amps()
    total_consumption = estimate_total_home_consumption_w()
    is_home_charging = is_tesla_home_charging(total_consumption)
    tesla_state["home_charging"] = bool(is_home_charging)
    # 以降の制御で使う is_charging は「自宅充電中」の意味に限定する。
    # Superchargerや外部充電中はFalseとして扱い、HEMSからTesla電流を触らない。
    is_charging = tesla_amp_control_active(total_consumption)

    # TeslaがChargingではなくても、充電器経由で空調・温調・補機電力を使っている場合がある。
    # この状態は通常充電制御の対象ではないが、ブレーカー保護上はTesla系負荷として扱う。
    tesla_aux_active = is_tesla_aux_power_active()

    # A案: POST側の既存 update_tesla_state は互換維持。
    # Wall Connector + Fleet分類が有効な場合は charging/charging_with_climate のときだけTrueを送る。
    # climate_only はPOST側の蓄電池防衛ロジックへは送らず、READ側の制御判断だけで扱う。
    if ev_wall_state.get("api_ok"):
        post_charging_state = wall_connector_should_notify_charging()
        notify_post_tesla_charging_state(post_charging_state, reason=f"control_loop_wall_{ev_wall_state.get('load_type')}")
    else:
        # TeslaMate取得コードは残すが、POST側への充電状態同期には使わない。
        # Wall Connectorが取れない時は安全側に倒して、POST側の蓄電池防衛を誤発火させない。
        notify_post_tesla_charging_state(False, reason="control_loop_wall_unavailable_no_teslamate_fallback")

    max_amps = int(sys_settings.get("MAX_CHARGE_AMPS", 32))
    min_amps = int(settings.get("solar_priority_min_amps", 5))
    safe_power = float(sys_settings.get("TARGET_SAFE_POWER_W", 6000))
    c_limit = float(sys_settings.get("CRITICAL_LIMIT_W", 6350))
    w_limit = float(sys_settings.get("WARNING_LIMIT_W", 6200))

    # 安価充電時間帯は、充電量最大化のため6.4kW直下を狙う。
    # 非安価時間帯は従来どおり TARGET_SAFE_POWER_W=6000W を安全目標にする。
    if cheap_window_grid_maximize_active(now):
        grid_targets = get_cheap_window_grid_targets(now)
        safe_power = float(grid_targets.get("grid_target_w", safe_power))
        c_limit = float(grid_targets.get("critical_limit_w", c_limit))
        w_limit = float(grid_targets.get("warning_limit_w", w_limit))

    # --- Tesla待機時A設定リセット ---
    # solar window end以降など、安価充電時間帯外でTeslaが充電中ではない場合のみ、
    # 32Aなどの残留設定を5Aへ戻す。configured charge-window start開始時点の32Aスタートは邪魔しない。
    try:
        reset_tesla_idle_amps_if_needed(now)
    except Exception as e:
        add_log("Tesla待機電流リセットエラー", str(e), "warning")

    # --- 緊急ブレーカー保護 ＆ 警告時の段階降下（最優先） ---
    # Charging中だけでなく、空調/温調などで充電器経由の補機電力を使っている状態も保護対象。
    # Battery First中でもブレーカー保護は絶対にスキップしない。
    #
    # v16: 6.4kW超過が続く場合は「1回下げて終わり」ではなく、
    #      Wall Connector/HEMSの次回実測でも上限超過なら、短いretry間隔でさらに下げる。
    #      通常の tesla_amp_decrease_cooldown_sec は予測追従用であり、
    #      ブレーカー保護のcritical時には待たない。
    if is_charging or tesla_aux_active:
        critical_retry_sec = max(1, int(settings.get("tesla_breaker_critical_retry_sec", 5) or 5))
        warning_retry_sec = max(1, int(settings.get("tesla_breaker_warning_retry_sec", 15) or 15))
        force_min_drop_a = max(1, int(settings.get("tesla_breaker_force_min_drop_a", 1) or 1))

        critical_retry_ok = (
            g_last_tesla_critical_drop_time is None
            or (now - g_last_tesla_critical_drop_time).total_seconds() >= critical_retry_sec
        )
        warning_retry_ok = (
            g_last_decrease_time is None
            or (now - g_last_decrease_time).total_seconds() >= warning_retry_sec
        )

        if current_grid >= c_limit and critical_retry_ok:
            # safe_power は安価時間帯なら概ね 6300W、通常時なら TARGET_SAFE_POWER_W。
            # 1A≒200Wとして、現在の超過分を一度で戻す。
            # v17: Wall実測がまだ32Aのままでも、直近指令31Aを基準に次は30Aへ落とす。
            breaker_current_amps = get_tesla_breaker_effective_current_amps(current_amps)
            excess = max(0.0, current_grid - safe_power)
            reduce_amps = max(force_min_drop_a, math.ceil(excess / 200.0))
            new_amps = max(min_amps, int(breaker_current_amps) - int(reduce_amps))

            if new_amps < int(breaker_current_amps):
                wall_a = get_wall_connector_current_a()
                add_log(
                    "買電パワー臨界超過",
                    f"買電 {int(current_grid)}W >= {int(c_limit)}W。テスラを {int(breaker_current_amps)}A ➔ {int(new_amps)}A に緊急降下します。Wall実測 {int(wall_a)}A。継続超過時は{critical_retry_sec}秒後に再降下します",
                    "critical"
                )
                request_tesla_amps_change(new_amps, force=True, reason="breaker_guard_critical_repeat")
                g_last_tesla_critical_drop_time = now
                g_last_decrease_time = now
                g_warning_start_time = None
                return
            else:
                add_log(
                    "買電パワー臨界超過",
                    f"買電 {int(current_grid)}W >= {int(c_limit)}W ですが、Teslaは既に最低{min_amps}A付近です",
                    "critical"
                )
                g_last_tesla_critical_drop_time = now

        if current_grid >= w_limit:
            if g_warning_start_time is None:
                g_warning_start_time = now
            elif (now - g_warning_start_time).total_seconds() >= 15 and warning_retry_ok:
                breaker_current_amps = get_tesla_breaker_effective_current_amps(current_amps)
                excess = max(0.0, current_grid - safe_power)
                reduce_amps = max(force_min_drop_a, math.ceil(excess / 200.0))
                new_amps = max(min_amps, int(breaker_current_amps) - int(reduce_amps))
                if new_amps < int(breaker_current_amps):
                    wall_a = get_wall_connector_current_a()
                    add_log(
                        "買電パワー警告超過",
                        f"買電 {int(current_grid)}W >= {int(w_limit)}W が15秒継続。テスラを {int(breaker_current_amps)}A ➔ {int(new_amps)}A に段階降下します。Wall実測 {int(wall_a)}A",
                        "warning"
                    )
                    # 警告域の降下もブレーカー保護なのでforce。
                    # ただしretryはcriticalより長めにしてチャタリングを避ける。
                    request_tesla_amps_change(new_amps, force=True, reason="breaker_guard_warning_repeat")
                    g_last_decrease_time = now
                    g_warning_start_time = None
                    return
        else:
            g_warning_start_time = None

    # --- 契約充電枠終了後の蓄電池自動復帰リセット ---
    if is_window_end_recovery_due("primary_charge_window", now):
        if g_last_reset_hour != int(get_window_end_hour_float("primary_charge_window", now, fallback=-1)):
            try:
                requests.post(post_control_url("/api/command"), json={"action": "auto"}, timeout=2)
                g_ctrl_status["status"] = "auto"
                g_last_reset_hour = int(get_window_end_hour_float("primary_charge_window", now, fallback=-1))
                add_log("システム自動復帰", "契約充電枠が終了したため、蓄電池をAutoへ戻しました", "success")
            except Exception as e:
                add_log("Auto復帰失敗", str(e), "critical")

    if is_window_end_recovery_due("solar_assist_charge_window", now):
        solar_end_marker = int(get_window_end_hour_float("solar_assist_charge_window", now, fallback=-2))
        if g_last_reset_hour != solar_end_marker:
            try:
                requests.post(post_control_url("/api/command"), json={"action": "auto"}, timeout=2)
                g_ctrl_status["status"] = "auto"
                g_day_standby_by_auto = False
                g_last_reset_hour = solar_end_marker
                add_log("制御枠終了", "契約上の充電枠が終了したため、蓄電池をAutoへ戻しました", "success")
            except Exception as e:
                add_log("Auto復帰失敗", str(e), "critical")

    if is_after_solar_window(now):
        if g_day_standby_by_auto and get_effective_battery_mode(g_ctrl_status) == "standby":
            try:
                requests.post(post_control_url("/api/command"), json={"action": "auto"}, timeout=2)
                g_ctrl_status["status"] = "auto"
                g_day_standby_by_auto = False
                add_log("Standby解除", "契約充電枠の一時Standbyが残っていたためAutoへ戻しました", "success")
            except Exception as e:
                add_log("Standby解除失敗", str(e), "critical")

    # --- Tesla不足量 ---
    tesla_missing_km = get_tesla_missing_km()
    heavy_charge_km = float(sys_settings.get("TESLA_HEAVY_CHARGE_KM", 30))
    tesla_needs_heavy_charge = (tesla_missing_km >= heavy_charge_km)
    forecast_data["tesla_needs_heavy_charge"] = tesla_needs_heavy_charge

    # --- 共通の家の消費電力計算 ---
    # Teslaが自宅充電ではない場合、Supercharger等の電力を家の消費から引かない。
    pure_consumption = get_pure_home_consumption_w(total_consumption)

    is_day = is_solar_assist_charge_window(now)
    is_night = is_primary_charge_window(now)

    # --- solar-assist window early phase Tesla優先 ---
    # この時間帯はSmart PV Multi側が自己診断で蓄電池を実質動かせないため、
    # 蓄電池Charge/Standbyは触らず、Teslaへ流せる分を流す。
    # ただし、小不足かつ太陽光だけで十分に回復できる場合は買電Boostしない。
    if is_day and is_solar_window_early_phase(now):
        tesla_auto_allowed = settings.get("tesla_auto_mode", True) and settings.get("tesla_charge_mode", "auto") != "manual"
        if not tesla_auto_allowed:
            return

        try:
            early_afternoon = estimate_afternoon_recovery(now, pure_consumption)
            early_schedule = calculate_day_energy_schedule(
                now, pure_consumption, tesla_missing_km, early_afternoon, is_charging=is_charging
            )
            early_strategy = early_schedule.get("strategy", "")
        except Exception:
            early_strategy = ""

        if is_charging:
            if bool(settings.get("day_early_respect_solar_recovery", True)) and early_strategy == "solar_recovery":
                target_a = calc_tesla_amps_for_solar_recovery(pure_consumption)
                apply_tesla_amp_target(target_a, current_amps, now, "solar-assist window early phase Solar Recovery: 買電Boostなし")
            else:
                target_a = calc_tesla_amps_for_grid_target(
                    pure_consumption,
                    target_grid_w=float(settings.get("day_early_tesla_target_grid_w", 6400))
                )
                apply_tesla_amp_target(target_a, current_amps, now, "solar-assist window early phase Tesla優先 200V32A上限")
            return

        # 停止中でも、Teslaが開始を受け付ける見込みがある場合だけ開始要求する。
        # 388km超など開始拒否されやすい領域では何もしない。
        if can_send_tesla_start(min_missing_km=float(settings.get("tesla_start_min_missing_km", 8))):
            target_a = calc_tesla_amps_for_grid_target(
                pure_consumption,
                target_grid_w=float(settings.get("day_early_tesla_target_grid_w", 6400))
            )
            if request_tesla_command("tesla_start"):
                g_last_tesla_start_time = now
                add_log("solar window startTesla開始", f"蓄電池自己診断時間帯のためTeslaへ開始要求。目安 {target_a}A / range {tesla_state.get('range')}km", "info")
                request_tesla_amps_change(target_a, reason="solar_window_start early tesla priority start")
        return

    # --- Battery First hard guard ---
    # Flow SummaryでBattery Firstなのに実機がauto/standbyになる矛盾を防ぐ。
    # solar assist windowでBattery FirstかつSOC未達なら、ここで必ず蓄電池Chargeを維持してreturnする。
    #
    # 注意:
    # このガードは制御ループの早い位置で動くため、pure_consumption 等が
    # まだ未定義の可能性がある。ここでは必ずfallback計算してから使う。
    try:
        bf_total_consumption = locals().get("total_consumption", estimate_total_home_consumption_w())
        # Teslaが自宅充電中ならTesla分だけ家庭純消費から除外する
        try:
            is_tesla_home_charging(bf_total_consumption)
        except Exception:
            pass
        bf_pure_consumption = locals().get("pure_consumption", get_pure_home_consumption_w(bf_total_consumption))
        bf_current_amps = locals().get(
            "current_amps",
            max(
                int(tesla_state.get("request_current", 0) or 0),
                int(tesla_state.get("actual_current", 0) or 0)
            )
        )
        bf_is_charging = locals().get(
            "is_charging",
            tesla_amp_control_active(bf_total_consumption)
        )

        bf_required, bf_reason, bf_target_soc, bf_afternoon = is_day_battery_first_required(now, bf_pure_consumption)
        if bf_required:
            if force_battery_first_cycle(
                now,
                bf_total_consumption,
                bf_pure_consumption,
                bf_current_amps,
                bf_is_charging,
                bf_reason,
                bf_target_soc,
                bf_afternoon
            ):
                return
    except Exception as e:
        add_log("Battery First hard guardエラー", str(e), "warning")


    is_day = is_solar_assist_charge_window(now)
    is_night = is_primary_charge_window(now)

    batt_req_w = 0.0
    skip_day_boost = False
    afternoon_info = estimate_afternoon_recovery(now, pure_consumption)

    # 残りソーラーで家・蓄電池・Teslaをすべて賄えるなら、solar assist windowの買電ブーストを抑える
    if is_day:
        # Day Priority Modeを最優先で実制御に適用する。
        # Battery First中はここでreturnし、旧Day Targetクリア処理によるStandby化を防ぐ。
        try:
            afternoon_for_priority = estimate_afternoon_recovery(now, pure_consumption)
            tesla_missing_for_priority = tesla_rem_kwh / 0.145 if tesla_rem_kwh > 0 else 0
            if enforce_day_priority_mode(
                now,
                total_consumption,
                pure_consumption,
                current_amps,
                is_charging,
                tesla_missing_for_priority,
                afternoon_for_priority
            ):
                return
        except Exception as e:
            add_log("Day Priority制御エラー", str(e), "warning")

    if is_day:
        remaining_solar_kwh = sum([h.get("gen", 0) for h in forecast_data.get("today_hourly", []) if int(h.get("time", "00").split(":")[0]) >= now.hour])
        tesla_rem_kwh = (tesla_missing_km * 145) / 1000.0
        house_rem_kwh = (pure_consumption / 1000.0) * max(0, int(settings.get("afternoon_target_hour", 17)) - now.hour)
        batt_rem_kwh = max(0, (int(settings.get("afternoon_target_soc", 70)) - state["soc"]) / 100.0 * (float(sys_settings.get("BATT_MAX_WH", 12600)) / 1000.0))
        total_needed_kwh = tesla_rem_kwh + house_rem_kwh + batt_rem_kwh
        if remaining_solar_kwh >= total_needed_kwh and total_needed_kwh > 0:
            skip_day_boost = True
            if g_last_skip_log_time is None or (now - g_last_skip_log_time).total_seconds() > 3600:
                add_log("AI買電スキップ発動", f"予測発電({remaining_solar_kwh:.1f}kWh)が要求({total_needed_kwh:.1f}kWh)を上回るため余剰優先に切替", "success")
                g_last_skip_log_time = now

    # ==================================================
    # 🌙 primary制御 (Primary Charge Window)
    # ==================================================
    if is_night:
        night_target_soc = int(g_ctrl_status.get("night_threshold", 30))
        hysteresis = max(1, int(settings.get("night_target_hysteresis_percent", 3)))
        night_release_soc = min(100, night_target_soc + hysteresis)
        current_mode = get_effective_battery_mode(g_ctrl_status)

        # 重要:
        # 旧ロジックは target 到達直後に auto へ戻していたため、
        # 30%到達 → Auto放電 → 29% → Charge → 30%... のチャタリングが発生した。
        # 新ロジックでは、target を下限、target+hysteresis を解除点として扱い、
        # 一度Chargeに入ったら release_soc まで充電して、その後はprimary window endまでStandbyで保持する。

        if state["soc"] < night_target_soc:
            # 下限を割ったらCharge開始
            request_battery_command("charge")
            hours_left = hours_until_window_end("primary_charge_window", now)
            req_wh = max(0, (night_release_soc - state["soc"]) / 100.0 * float(sys_settings.get("BATT_MAX_WH", 12600)))
            batt_req_w = min(5000.0, req_wh / hours_left)

        elif current_mode == "charge" and state["soc"] < night_release_soc:
            # 充電中はtargetで止めず、ヒステリシス上限まで継続
            hours_left = hours_until_window_end("primary_charge_window", now)
            req_wh = max(0, (night_release_soc - state["soc"]) / 100.0 * float(sys_settings.get("BATT_MAX_WH", 12600)))
            batt_req_w = min(5000.0, req_wh / hours_left)

        else:
            batt_req_w = 0.0
            # Tesla充電中、または一度Chargeしてreleaseに達した場合、またはSOCがrelease近辺ならStandby保持。
            # 安価時間帯に蓄電池を家庭負荷へ放電させて再Chargeするのを避ける。
            should_hold_standby = is_charging or current_mode == "charge" or state["soc"] <= night_release_soc
            if should_hold_standby:
                if get_effective_battery_mode(g_ctrl_status) != "standby":
                    request_battery_command("standby")
                    reason = "テスラ充電中のためStandbyで枠を開放します" if is_charging else f"Night Target到達。{night_release_soc}%までは再放電させずStandby保持します"
                    add_log("primarySOC保持", reason, "success")
            else:
                # 十分SOCが高い場合のみAutoを許可。primary window endには別処理でAuto復帰する。
                if get_effective_battery_mode(g_ctrl_status) != "auto":
                    request_battery_command("auto")

    # ==================================================
    # ☀️ solar assist制御 (Solar Assist Window)
    # ==================================================
    elif is_day:
        day_target_soc = get_effective_day_target(now)
        recovery_mode = bool(settings.get("afternoon_recovery_mode", True))
        can_recover_after_solar_window = recovery_mode and afternoon_info.get("can_recover_after_solar_window", False)

        # solar-assist window early phaseは蓄電池制御を触らない。Teslaがsolar window start前から充電している場合を優先。
        if is_solar_window_early_phase(now):
            batt_req_w = 0.0
        else:
            if skip_day_boost or can_recover_after_solar_window:
                # 午後ソーラーで夕方Targetまで回復できるなら、solar assist windowに蓄電池Chargeを強制しない。
                batt_req_w = 0.0
                if is_charging:
                    if get_effective_battery_mode(g_ctrl_status) != "standby":
                        request_battery_command("standby")
                        g_day_standby_by_auto = True
                        add_log("午後回復見込み", "solar window end以降の太陽光で夕方Targetへ回復可能。Tesla優先のため蓄電池Standby", "success")
                else:
                    # Teslaが充電していないならStandby保持は不要。
                    # SOCが100%未満ならAutoで太陽光を蓄電池へ吸収させる。
                    if state["soc"] < 100 and get_effective_battery_mode(g_ctrl_status) != "auto":
                        if request_battery_command("auto"):
                            g_day_standby_by_auto = False
                            add_log("午後回復見込み", "Tesla停止中のため、蓄電池をAutoへ戻して太陽光を吸収します", "info")
            else:
                # 午後だけでは夕方Targetに足りない。solar window endまでに必要分を積む。
                required_pre_solar_window_soc = max(day_target_soc, int(afternoon_info.get("required_pre_solar_window_soc", day_target_soc)))
                required_pre_solar_window_soc = min(100, required_pre_solar_window_soc)
                hours_left = hours_until_window_end("solar_assist_charge_window", now)
                req_wh = max(0, (required_pre_solar_window_soc - state["soc"]) / 100.0 * float(sys_settings.get("BATT_MAX_WH", 12600)))

                soc_deadband = max(0, int(settings.get("battery_soc_decision_deadband_percent", 2)))
                if req_wh > 0 and state["soc"] < max(0, required_pre_solar_window_soc - soc_deadband):
                    request_battery_command("charge")
                    batt_req_w = min(5000.0, req_wh / hours_left)
                else:
                    batt_req_w = 0.0
                    is_day_standby_cooldown = (g_last_day_standby_time and (now - g_last_day_standby_time).total_seconds() < 300)

                    # Teslaが充電中ならStandbyで蓄電池からTeslaへ流れるのを防ぐ。
                    # Teslaが停止/充電完了しているならAutoに戻し、太陽光を蓄電池へ吸わせる。
                    if is_charging:
                        if get_effective_battery_mode(g_ctrl_status) == "charge" and not is_day_standby_cooldown:
                            if request_battery_command("standby"):
                                g_last_day_standby_time = now
                                g_day_standby_by_auto = True
                                add_log("solar assist蓄電池目標クリア", "Tesla充電中のため、蓄電池を一時Standbyにして吸い出しを防止します", "success")
                    else:
                        if state["soc"] < 100 and get_effective_battery_mode(g_ctrl_status) != "auto":
                            if request_battery_command("auto"):
                                g_day_standby_by_auto = False
                                add_log("solar assist余剰吸収へ復帰", "Tesla充電が停止中のため、蓄電池をAutoへ戻して太陽光を吸収します", "success")

    # --- solar assist window Standby残留解除 ---
    # Tesla充電が終わっているのにStandbyが残ると、太陽光が売電されてしまう。
    # solar window endを待たず、SOCが100%未満ならAutoへ戻して余剰を吸収する。
    if is_day and not is_charging and get_effective_battery_mode(g_ctrl_status) == "standby" and state["soc"] < 100:
        if request_battery_command("auto"):
            g_day_standby_by_auto = False
            add_log("solar assistStandby解除", "Tesla充電停止中かつ蓄電池に空きがあるため、Autoへ戻して太陽光を吸収します", "success")


    # --- solar assist window Charge残留解除 ---
    # Tesla充電が終わっていて、蓄電池がAfternoon Target付近へ到達済みなら、
    # Chargeのまま残さずAutoへ戻す。
    # 目的:
    #   - グリッド充電完了後にCharge/Standbyが残り、太陽光が売電へ逃げるのを防ぐ
    #   - solar window endを待たず、時間内でも余剰太陽光を蓄電池へ吸収させる
    if is_day and not is_charging and state.get("soc", 0) < 100:
        try:
            afternoon_target = get_battery_first_target_soc(afternoon_info if "afternoon_info" in locals() else None)
            release_margin = int(settings.get("day_priority_release_margin_percent", 1))
            current_mode = get_effective_battery_mode(g_ctrl_status)
            if (
                current_mode == "charge"
                and state.get("soc", 0) >= max(0, int(afternoon_target) - release_margin)
            ):
                if request_battery_command("auto"):
                    g_day_standby_by_auto = False
                    add_log(
                        "solar assistCharge解除",
                        f"蓄電池が目標SOC {afternoon_target}% 付近に到達し、Teslaも完了しているためAutoへ戻します",
                        "success"
                    )
        except Exception as e:
            add_log("solar assistCharge解除判定エラー", str(e), "warning")

    # --- 自宅外充電ガード ---
    # Superchargerや外部普通充電中は、HEMSからTesla電流変更/停止/開始を送らない。
    # 蓄電池のprimary/solar assist制御は継続するが、Teslaは制御対象外として扱う。
    if is_tesla_charging and not is_home_charging and settings.get("tesla_control_only_when_home_charging", True):
        if g_last_skip_log_time is None or (now - g_last_skip_log_time).total_seconds() > 1800:
            add_log(
                "Tesla自宅外充電検出",
                f"TeslaはChargingですが自宅負荷に見えないため制御対象外にします: {tesla_state.get('home_charge_reason')}",
                "info"
            )
            g_last_skip_log_time = now
        return

    # ==================================================
    # ⚡ Tesla 制御ブロック (tesla_auto_mode OFF ならここで終了)
    # ==================================================
    if not settings.get("tesla_auto_mode", True):
        return

    tesla_mode = settings.get("tesla_charge_mode", "auto")
    if tesla_mode not in ["auto", "solar_priority", "manual"]:
        tesla_mode = "auto"

    # --- テスラの安価時間帯制御 ---
    if is_charging and (is_day or is_night):
        if tesla_mode == "manual":
            return

        # 6.4kW直下まで余白がある場合、Wall Connector実測Aを基準に少しずつ増やす。
        # 後段のJIT計算もsafe_power=約6.3kWを使うため、ここは実測フィードバック補助。
        maybe_raise_tesla_amps_to_grid_limit(
            current_grid,
            current_amps,
            now,
            "Cheap Window Grid Maximize"
        )

        if is_day and tesla_mode == "solar_priority":
            allowed_grid_assist_w = int(settings.get("solar_priority_grid_assist_w", 200))
            surplus_w = state["solar"] - pure_consumption

            # 午後に蓄電池回復可能、かつTesla不足が大きい場合はsolar assist windowの安価枠で最大Aを許容
            if tesla_needs_heavy_charge and (skip_day_boost or afternoon_info.get("can_recover_after_solar_window", False)):
                target_amps = max_amps
                reason = "Tesla不足大・午後蓄電池回復見込みあり"
            else:
                # 蓄電池に必要な予約電力がある場合は差し引く
                available_w = surplus_w + allowed_grid_assist_w
                if batt_req_w > 0:
                    available_w = (safe_power + state["solar"]) - pure_consumption - batt_req_w
                target_amps = max(min_amps, min(max_amps, math.floor(max(0, available_w) / 200)))
                reason = f"Solar Priority / assist {allowed_grid_assist_w}W"

            apply_tesla_amp_target(target_amps, current_amps, now, reason)

        else:
            # 従来寄りのJIT制御。蓄電池予約電力を差し引いた残りをTeslaへ渡す。
            available_w = (safe_power + state["solar"]) - pure_consumption - batt_req_w
            ideal_amps = math.floor(max(0, available_w) / 200)
            target_amps = max(min_amps, min(max_amps, ideal_amps))

            # solar-assist window early phaseかつTesla不足が大きい場合、蓄電池を触らずTesla最大寄りを許容
            if is_day and is_solar_window_early_phase(now) and tesla_needs_heavy_charge and not skip_day_boost:
                target_amps = max_amps

            apply_tesla_amp_target(target_amps, current_amps, now, "Cheap Window JIT")

    # --- その他の時間帯：太陽光の余剰追従のみ (SmartCharge) ---
    elif not (is_day or is_night):
        if tesla_mode == "manual":
            return

        surplus_w = state["solar"] - pure_consumption
        tesla_min_soc = settings.get("tesla_min_soc", 50)

        # solar window end以降は蓄電池Autoが自然。Tesla充電中でも通常フローへ戻す。
        if is_after_solar_window(now) and get_effective_battery_mode(g_ctrl_status) == "standby" and not g_day_standby_by_auto:
            request_battery_command("auto")

        if state["solar"] > 100 and surplus_w >= 1000 and state["battery"] >= -100:
            g_low_surplus_start_time = None
            target_amps = math.floor(surplus_w / 200)
            target_amps = min(max_amps, max(min_amps, target_amps))

            if not is_charging:
                # TeslaがChargingではないが補機/空調で充電器電力を使っている時は、
                # 車両側が状態遷移中の可能性があるため、通常時間帯のSmartCharge開始要求は見送る。
                # solar assist window/primary charge windowの計画充電は別ブロックで扱う。
                if tesla_aux_active:
                    add_debug("SmartCharge開始抑止", "Tesla補機/空調電力中のため通常時間帯の開始要求を見送ります", throttle_sec=300)
                elif state["soc"] >= tesla_min_soc:
                    if can_send_tesla_start():
                        g_last_tesla_start_time = now
                        g_tesla_start_retries += 1
                        add_log(f"余剰 {math.floor(surplus_w)}W 検出", "テスラへ充電開始要求を送信", "info")
                        request_tesla_command("tesla_start")
                        if g_tesla_start_retries >= 3:
                            g_tesla_suspended = True
                            add_log("SmartCharge異常", "3回連続失敗のためサスペンド", "critical")
                            send_discord_notify("⚠️ SmartCharge自動開始に3回失敗しました！状態を確認してね。")
                        time.sleep(2)
                        request_tesla_amps_change(target_amps, reason="after_start")
                        g_last_increase_time = now
            else:
                if state["soc"] < tesla_min_soc:
                    add_log("蓄電池残量低下", "SmartCharge停止、太陽光を蓄電池へ回します", "warning")
                    request_tesla_command("tesla_stop")
                    g_last_decrease_time = now
                else:
                    apply_tesla_amp_target(target_amps, current_amps, now, "SmartCharge surplus follow")
        else:
            if is_charging:
                if current_amps > min_amps:
                    if not (g_last_decrease_time and (now - g_last_decrease_time).total_seconds() < 60):
                        add_log("余剰低下・蓄電池放電", f"テスラ電流を最低{min_amps}Aへ降下します", "warning")
                        request_tesla_amps_change(min_amps, reason="low_surplus_min")
                        g_last_decrease_time = now

                if g_low_surplus_start_time is None:
                    g_low_surplus_start_time = now
                elif (now - g_low_surplus_start_time).total_seconds() >= 300:
                    add_log("余剰不足・蓄電池放電5分継続", "テスラ充電を停止します", "warning")
                    request_tesla_command("tesla_stop")
                    g_low_surplus_start_time = None
            else:
                g_low_surplus_start_time = None

# ==========================================
# 🌤️ 予測ロジック (バックグラウンド)
# ==========================================
def update_forecast_worker():
    location = Location(LAT, LON, tz='Asia/Tokyo')
    time.sleep(5)
    
    while True:
        try:
            if g_read_only_mode:
                time.sleep(60); continue
                
            now = pd.Timestamp.now(tz='Asia/Tokyo').floor('h')
            url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&hourly=temperature_2m,cloudcover,precipitation&timezone=Asia%2FTokyo&forecast_days=2"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                raise ValueError(f"Open-Meteo non-JSON: status={response.status_code}, content-type={content_type}, body={response.text[:120]!r}")
            res = response.json()
            if "hourly" not in res:
                raise ValueError(f"Open-Meteo missing hourly: {str(res)[:200]}")
            df = pd.DataFrame(res['hourly'])
            df['time'] = pd.to_datetime(df['time']).dt.tz_localize('Asia/Tokyo')
            df.set_index('time', inplace=True)

            def get_hourly_generation(start, end):
                times = pd.date_range(start=start, end=end, freq='1h', tz='Asia/Tokyo')
                df_range = df.reindex(times)
                if df_range.empty or df_range['cloudcover'].isnull().all(): return pd.Series(0.0, index=times)
                clearsky = location.get_clearsky(times)
                ghi = clearsky['ghi'] * (1 - 0.75 * ((df_range['cloudcover'] / 100) ** 2))
                ghi.loc[(df_range['cloudcover'] >= 80).values] *= 0.5
                ghi.loc[(df_range['precipitation'] >= 0.5).values] *= 0.5
                cell_temp = pvlib.temperature.pvsyst_cell(ghi, df_range['temperature_2m'], wind_speed=1.0)
                temp_loss = 1 - 0.004 * (cell_temp - 25)
                
                cap_w = float(sys_settings.get("SYSTEM_CAPACITY_W", 2400))
                loss_f = float(sys_settings.get("SYSTEM_LOSS_FACTOR", 0.822))
                return (cap_w * (ghi / 1000) * temp_loss * loss_f) / 1000

            today_start = now.replace(hour=0)
            tomorrow_start = today_start + timedelta(days=1)
            
            df_today = get_hourly_generation(today_start, today_start.replace(hour=23))
            df_tomorrow = get_hourly_generation(tomorrow_start, tomorrow_start.replace(hour=23))

            solar_win = parse_contract_window("solar_assist_charge_window") or (now.hour * 60, now.hour * 60)
            solar_start_h = max(0, min(23, int(solar_win[0] / 60)))
            solar_end_h = max(0, min(23, int(solar_win[1] / 60)))
            if solar_end_h <= solar_start_h:
                solar_end_h = solar_start_h
            df_solar_window = get_hourly_generation(now.replace(hour=solar_start_h), today_start.replace(hour=solar_end_h))
            gen_solar_window = df_solar_window.sum() if not df_solar_window.empty else 0
            df_after_window = get_hourly_generation(now.replace(hour=solar_end_h), today_start.replace(hour=int(settings.get("afternoon_target_hour", 17))))
            gen_after_window = df_after_window.sum() if not df_after_window.empty else 0
            
            load_to_night = (14.0 / 24) * 16
            batt_max_wh = float(sys_settings.get("BATT_MAX_WH", 12600))
            
            needed_kwh_at_window_end = (load_to_night + ((batt_max_wh/1000) * 0.3)) - gen_after_window
            current_kwh = (batt_max_wh / 1000) * (state["soc"] / 100)
            predicted_kwh_at_window_end_solar_only = current_kwh + gen_solar_window

            if predicted_kwh_at_window_end_solar_only < needed_kwh_at_window_end:
                target_soc_window_end_rounded = round(min(100.0, (needed_kwh_at_window_end / (batt_max_wh/1000)) * 100))
                ai_needs_grid = True
            else:
                target_soc_window_end_rounded = round(min(100.0, (predicted_kwh_at_window_end_solar_only / (batt_max_wh/1000)) * 100))
                ai_needs_grid = False

            forecast_data["today_date"] = today_start.strftime("%m/%d")
            forecast_data["tomorrow_date"] = tomorrow_start.strftime("%m/%d")
            forecast_data["today_hourly"] = [{"time": f"{t.hour:02d}:00", "gen": float(g)} for t, g in df_today.items() if 5 <= t.hour <= 19]
            forecast_data["tomorrow_hourly"] = [{"time": f"{t.hour:02d}:00", "gen": float(g)} for t, g in df_tomorrow.items() if 5 <= t.hour <= 19]
            forecast_data["today_total"] = round(float(df_today.sum()), 2)
            forecast_data["tomorrow_total"] = round(float(df_tomorrow.sum()), 2)
            forecast_data["gen_afternoon_kwh"] = round(float(gen_after_window), 2)
            forecast_data["predicted_soc_window_end"] = target_soc_window_end_rounded
            forecast_data["ai_needs_grid"] = ai_needs_grid
            forecast_data["last_calc"] = now.strftime("%H:%M:%S")
            
            manual_day_active = g_manual_day_target_until and datetime.now(JST) < g_manual_day_target_until
            if settings["day_auto_mode"] and not manual_day_active:
                try:
                    requests.post(post_control_url("/api/command"), json={"action": "thre_day", "value": str(target_soc_window_end_rounded)}, timeout=5)
                except:
                    pass

            try:
                points = []
                for t, g_kw in df_today.items():
                    if t >= now: points.append(Point("power_forecast").tag("source", "open_meteo").field("predicted_solar", float(g_kw * 1000)).time(t.astimezone(timezone.utc), WritePrecision.S))
                for t, g_kw in df_tomorrow.items():
                    points.append(Point("power_forecast").tag("source", "open_meteo").field("predicted_solar", float(g_kw * 1000)).time(t.astimezone(timezone.utc), WritePrecision.S))
                if points: write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            except: pass
            
            time.sleep(3600)
        except Exception as e:
            print(f"🚨 予測ワーカーエラー: {e}")
            traceback.print_exc()
            time.sleep(300)

# ==========================================
# 🔌 デバイス通信 (Tesla MQTT & Websocket)
# ==========================================
def on_mqtt_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode().strip()
        key = topic.split("/")[-1]

        if key in ["battery_level", "usable_battery_level"]:
            tesla_state["soc"] = int(float(payload))
            if tesla_state["soc"] >= 90 and tesla_state["range"] > 0:
                tesla_state["max_range"] = round(tesla_state["range"] / (tesla_state["soc"] / 100.0), 1)

        elif key in ["ideal_battery_range_km", "rated_battery_range_km", "est_battery_range_km"]:
            # 既存UIとの互換のためrangeにはideal/rated系のkmを入れる。
            tesla_state["range"] = round(float(payload), 1)
            if tesla_state["soc"] >= 90 and tesla_state["range"] > 0:
                tesla_state["max_range"] = round(tesla_state["range"] / (tesla_state["soc"] / 100.0), 1)

        elif key == "charging_state":
            tesla_state["charging_state"] = payload
            if payload == "Charging":
                global g_tesla_start_retries, g_tesla_suspended
                g_tesla_start_retries = 0
                g_tesla_suspended = False

        elif key == "charger_power":
            # TeslaMate docs: charger_power is charger power, e.g. 48.9
            # Use this as kW. Keep tesla_state["power"] for existing UI compatibility.
            kw = float(payload)
            tesla_state["charger_power"] = kw
            tesla_state["power"] = kw

        elif key == "power":
            # TeslaMate docs: battery power in watts. Positive discharge, negative charge.
            # Do not use this for charging control power; charger_power is the correct source.
            tesla_state["battery_power_w"] = float(payload)

        elif key == "time_to_full_charge":
            tesla_state["time_to_full"] = float(payload)

        elif key == "charger_actual_current":
            tesla_state["actual_current"] = int(float(payload))

        elif key == "charge_current_request":
            tesla_state["request_current"] = int(float(payload))

        elif key == "charge_limit_soc":
            tesla_state["charge_limit"] = int(float(payload))

        elif key == "plugged_in":
            tesla_state["plugged_in"] = (payload.lower() == "true")

        elif key == "geofence":
            tesla_state["geofence"] = payload
            gf = normalize_geofence_name(payload)
            home_names = [normalize_geofence_name(x) for x in settings.get("tesla_home_geofence_names", [])]
            if gf:
                tesla_state["is_home"] = gf in home_names
            else:
                tesla_state["is_home"] = None

        elif key == "location":
            try:
                tesla_state["location"] = json.loads(payload)
            except Exception:
                tesla_state["location"] = payload

        # MQTT更新ごとに現在の制御許可状態を軽く更新する。
        total = estimate_total_home_consumption_w()
        tesla_state["home_charging"] = bool(is_tesla_home_charging(total))
        tesla_state["control_allowed"] = bool(can_control_tesla_now())

    except Exception as e:
        try:
            add_log("Tesla MQTT解析エラー", f"{msg.topic}: {e}", "warning")
        except Exception:
            pass

def mqtt_worker():
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except AttributeError:
        mqtt_client = mqtt.Client()
        
    mqtt_client.on_message = on_mqtt_message
    try:
        mqtt_client.connect(MQTT_HOST, 1883)
        mqtt_client.subscribe("teslamate/cars/1/#")
        mqtt_client.loop_forever()
    except:
        time.sleep(10); mqtt_worker()

def decode(edt_val, is_signed=True):
    b64 = edt_val.get("EDT") if isinstance(edt_val, dict) else edt_val
    if not isinstance(b64, str): return 0
    try:
        raw = base64.b64decode(b64)
        if len(raw) >= 4:
            raw = raw[-4:]
            val = int.from_bytes(raw, byteorder='big', signed=is_signed)
            return val if abs(val) < 100000 else 0
        return int.from_bytes(raw, byteorder='big', signed=is_signed)
    except: return 0

def on_message(ws, message):
    try:
        data = json.loads(message)
        msg_type = data.get("type")
        payload = data.get("payload", {})
        
        def update(epc, val):
            if epc == "E0": state["solar"] = decode(val)
            elif epc == "F1": state["grid"] = decode(val)
            elif epc == "D3": state["battery"] = decode(val) 
            elif epc == "E4": state["soc"] = decode(val, False)
            elif epc == "E2": state["rem_wh"] = decode(val, False)
            state["last_update"] = datetime.now(JST).strftime("%H:%M:%S")

        if msg_type == "property_changed":
            epc = payload.get("epc")
            update(epc, payload.get("value"))
            send_to_influx(state, tesla_state)
            if epc == "F1": check_grid_safety_limit()

        if msg_type == "initial_state":
            for _, dev in payload.get("devices", {}).items():
                for epc, val in dev.get("properties", {}).items(): update(epc, val)
    except: pass

def hems_worker():
    while True:
        try:
            ws = websocket.WebSocketApp(WS_URL, on_message=on_message, on_open=lambda w: threading.Thread(target=heartbeat, args=(w,), daemon=True).start())
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        except: pass
        time.sleep(5)

def heartbeat(ws):
    while True:
        try:
            if ws.sock and ws.sock.connected:
                ws.send(json.dumps({"type":"get_properties","payload":{"targets":[TARGET_SOLAR],"epcs":["E0"]}}))
                ws.send(json.dumps({"type":"get_properties","payload":{"targets":[TARGET_BATTERY],"epcs":["F1","D3","E4","E2"]}}))
        except: pass
        time.sleep(5)

def get_real_charge_speed_kmh(amps):
    speed_map = {0: 0, 5: 6, 10: 13, 15: 20, 20: 29, 24: 35, 32: 47}
    if amps in speed_map: return speed_map[amps]
    keys = sorted(speed_map.keys())
    for i in range(len(keys) - 1):
        if keys[i] < amps < keys[i+1]:
            x0, y0 = keys[i], speed_map[keys[i]]; x1, y1 = keys[i+1], speed_map[keys[i+1]]
            return y0 + (y1 - y0) * (amps - x0) / (x1 - x0)
    if amps > 32: return 47 + (amps - 32) * (47 - 35) / (32 - 24)
    return 0

def get_rate_info():
    now = datetime.now(JST)
    if is_cheap_charge_window(now):
        return {"is_cheap": True, "label": "Contract Charge Window", "timer": "Active"}
    return {"is_cheap": False, "label": "Standard Window", "timer": "Configured locally"}

def send_to_influx(data_state, t_state):
    if write_api is None:
        return
    try:
        total_cons = max(0, data_state["solar"] + data_state["grid"] - data_state["battery"])
        point = Point("power_stats").tag("device", env_str("INFLUX_DEVICE_TAG", "smart_hems")).field("solar", float(data_state["solar"])) \
            .field("grid", float(data_state["grid"])).field("battery", float(data_state["battery"])) \
            .field("soc", float(data_state["soc"])).field("consumption", float(total_cons)) \
            .field("tesla_soc", float(t_state["soc"])).field("tesla_power", float(t_state["power"]))
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except: pass

# ==========================================
# 🔄 5秒ごとのメイン制御ループ
# ==========================================
def control_worker():
    while True:
        try:
            check_grid_safety_limit()
        except Exception as e:
            add_log("制御ループエラー", str(e), "critical")
        time.sleep(5)

# ==========================================
# 📡 API Routing
# ==========================================
@app.route('/api/sys_settings', methods=['GET', 'POST'])
def handle_sys_settings():
    if request.method == 'GET':
        return jsonify(sys_settings)
    else:
        data = request.json
        if data.get("action") == "reset":
            sys_settings.update(DEFAULT_SETTINGS)
            save_sys_settings()
            add_log("システム設定", "設定値をデフォルトにリセットしました", "info")
            return jsonify({"status": "success", "settings": sys_settings})
        else:
            new_settings = data.get("settings", {})
            for k, v in new_settings.items():
                if v == "": continue
                if k in sys_settings:
                    if k == "DISCORD_WEBHOOK_URL":
                        sys_settings[k] = str(v)
                    else:
                        sys_settings[k] = float(v) if '.' in str(v) else int(v)
            save_sys_settings()
            add_log("システム設定", "新しい設定値を保存・適用しました", "success")
            return jsonify({"status": "success", "settings": sys_settings})



def forecast_kwh_from_list(hourly, start_hour, end_hour):
    total = 0.0
    try:
        for h in hourly or []:
            try:
                hour = int(str(h.get("time", "00:00")).split(":")[0])
                if start_hour <= hour < end_hour:
                    total += float(h.get("gen", 0) or 0)
            except Exception:
                pass
    except Exception:
        pass
    return total


def get_tesla_missing_km():
    try:
        ds = build_tesla_range_dataset(build_wall_connector_payload())
        missing = _safe_float(ds.get("missing_km"), None)
        if missing is not None:
            return max(0.0, missing)
    except Exception:
        pass
    try:
        if tesla_state["soc"] <= 0:
            return 0.0
        target_range = tesla_state["max_range"] * (tesla_state["charge_limit"] / 100.0)
        return max(0.0, target_range - tesla_state["range"])
    except Exception:
        return 0.0


def build_low_solar_proposal(now, pure_consumption):
    """前日夜〜当日に、低発電日のNight/Day Target推奨を返す。適用はユーザ操作。"""
    try:
        tomorrow_total = float(forecast_data.get("tomorrow_total", 0) or 0)
        today_total = float(forecast_data.get("today_total", 0) or 0)
        target_hour = int(settings.get("afternoon_target_hour", 17))
        afternoon_target = int(settings.get("afternoon_target_soc", 70))
        low_threshold = float(settings.get("low_solar_threshold_kwh", 5.0))
        storm_threshold = float(settings.get("storm_solar_threshold_kwh", 3.0))
        tesla_small_km = float(settings.get("proposal_tesla_small_km", 10))
        tesla_heavy_km = float(sys_settings.get("TESLA_HEAVY_CHARGE_KM", 30))
        tesla_missing_km = get_tesla_missing_km()

        # 次回運用向けの発電予測を表示材料として使う。
        forecast_date = forecast_data.get("tomorrow_date", "--/--")
        forecast_total = tomorrow_total
        hourly = forecast_data.get("tomorrow_hourly", [])

        dismissed_key = f"{forecast_date}"
        dismissed = settings.get("proposal_dismissed_date", "") == dismissed_key
        enabled = bool(settings.get("low_solar_proposal_mode", True))

        solar_end_hour = int(get_window_end_hour_float("solar_assist_charge_window", datetime.now(JST), fallback=datetime.now(JST).hour))
        solar_end_hour = max(0, min(23, solar_end_hour))
        target_hour = max(solar_end_hour + 1, min(23, target_hour))
        afternoon_solar = forecast_kwh_from_list(hourly, solar_end_hour, target_hour)
        house_after_solar_window = max(0.0, (pure_consumption / 1000.0) * max(0, target_hour - solar_end_hour))
        solar_for_battery = max(0.0, afternoon_solar - house_after_solar_window)
        batt_max_kwh = float(sys_settings.get("BATT_MAX_WH", 12600)) / 1000.0
        # ざっくり現在SOCからAfternoon Targetまでの必要量。primary運用では保守的に見る。
        battery_needed = max(0.0, (afternoon_target - state.get("soc", 0)) / 100.0 * batt_max_kwh)
        deficit = max(0.0, battery_needed - solar_for_battery)
        can_recover = deficit <= 0.1

        if forecast_total <= 0:
            level = "info"
            title = "明日の発電予測を取得中です"
            active = False
        elif forecast_total < storm_threshold:
            level = "critical"
            title = "明日はかなり発電が少ない予測です"
            active = True
        elif forecast_total < low_threshold or not can_recover:
            level = "warning"
            title = "明日は低発電日の可能性があります"
            active = True
        else:
            level = "success"
            title = "明日は通常運用でよさそうです"
            active = False

        if not enabled:
            active = False
            title = "提案モードはOFFです"
            level = "info"

        if dismissed:
            active = False
            title = "今日の提案は無視に設定されています"
            level = "info"

        if forecast_total < storm_threshold:
            rec_night = int(settings.get("proposal_storm_night_target", 60))
        elif forecast_total < low_threshold or not can_recover:
            rec_night = int(settings.get("proposal_low_night_target", 50))
        else:
            rec_night = int(g_ctrl_status.get("night_threshold", 25))

        rec_day = int(settings.get("proposal_low_day_target", 100)) if active else int(g_ctrl_status.get("day_threshold", forecast_data.get("predicted_soc_window_end", 50)))

        reasons = []
        if forecast_total > 0:
            reasons.append(f"明日発電予測は {forecast_total:.1f}kWh です。")
        if not can_recover:
            reasons.append(f"solar window end以降の太陽光余力 {solar_for_battery:.1f}kWh に対し、蓄電池必要量 {battery_needed:.1f}kWh、差分 {deficit:.1f}kWh です。")
        if tesla_missing_km <= tesla_small_km:
            reasons.append(f"Tesla不足は {tesla_missing_km:.0f}km 程度で、セントリーモード等の自然減少レベルです。蓄電池優先を推奨します。")
        elif tesla_missing_km >= tesla_heavy_km:
            reasons.append(f"Tesla不足が {tesla_missing_km:.0f}km あり、solar assist windowのTesla充電枠も考慮が必要です。")
        else:
            reasons.append(f"Tesla不足は {tesla_missing_km:.0f}km です。蓄電池優先を基本に、余力があればTeslaへ回します。")

        message = ""
        if active:
            message = f"Night Target {g_ctrl_status.get('night_threshold', '--')}% → {rec_night}%、Day Target {g_ctrl_status.get('day_threshold', '--')}% → {rec_day}% を推奨します。"
        elif level == "success":
            message = "明日の発電予測では、特別なNight Target引き上げは不要そうです。"

        return {
            "enabled": enabled,
            "active": active,
            "dismissed": dismissed,
            "level": level,
            "title": title,
            "message": message,
            "forecast_date": forecast_date,
            "forecast_total_kwh": round(forecast_total, 2),
            "today_total_kwh": round(today_total, 2),
            "tomorrow_total_kwh": round(tomorrow_total, 2),
            "recommended_night_target": rec_night,
            "recommended_day_target": rec_day,
            "current_night_target": g_ctrl_status.get("night_threshold"),
            "current_day_target": g_ctrl_status.get("day_threshold"),
            "afternoon_target_soc": afternoon_target,
            "afternoon_target_hour": target_hour,
            "afternoon_solar_kwh": round(afternoon_solar, 2),
            "house_after_solar_window_kwh": round(house_after_solar_window, 2),
            "solar_for_battery_kwh": round(solar_for_battery, 2),
            "battery_needed_kwh": round(battery_needed, 2),
            "deficit_kwh": round(deficit, 2),
            "can_recover_after_solar_window": can_recover,
            "tesla_missing_km": round(tesla_missing_km, 1),
            "tesla_small_km": tesla_small_km,
            "tesla_heavy_km": tesla_heavy_km,
            "reasons": reasons,
        }
    except Exception as e:
        return {
            "enabled": bool(settings.get("low_solar_proposal_mode", True)),
            "active": False,
            "level": "warning",
            "title": "提案計算に失敗しました",
            "message": str(e),
            "reasons": [],
        }

def build_solar_priority_warning(now, pure_consumption):
    """Solar Priority運用時に、蓄電池不足リスクをUIへ返す。"""
    try:
        if settings.get("tesla_charge_mode", "auto") != "solar_priority":
            return {
                "active": False,
                "level": "info",
                "title": "",
                "message": "",
                "reasons": []
            }

        afternoon = estimate_afternoon_recovery(now, pure_consumption)
        tesla_missing_km = 0
        if tesla_state["soc"] > 0 and tesla_state["soc"] < tesla_state["charge_limit"]:
            target_range = tesla_state["max_range"] * (tesla_state["charge_limit"] / 100.0)
            tesla_missing_km = max(0, target_range - tesla_state["range"])

        reasons = []
        if not afternoon.get("can_recover_after_solar_window", False):
            reasons.append(
                f"solar window end以降の太陽光余力 {afternoon.get('solar_for_battery_kwh', 0):.1f}kWh に対し、"
                f"蓄電池必要量 {afternoon.get('battery_needed_kwh', 0):.1f}kWh です。"
            )

        if tesla_missing_km >= float(sys_settings.get("TESLA_HEAVY_CHARGE_KM", 30)):
            reasons.append(f"Tesla不足が {tesla_missing_km:.0f}km あり、充電要求が大きい状態です。")

        if state["soc"] < afternoon.get("required_pre_solar_window_soc", 0):
            reasons.append(
                f"solar window endまでに最低 {afternoon.get('required_pre_solar_window_soc', 0)}% 近く欲しい見込みですが、"
                f"現在SOCは {state['soc']}% です。"
            )

        if reasons:
            return {
                "active": True,
                "level": "warning",
                "title": "今日はSolar Priorityだけだと蓄電池が不足する可能性があります",
                "message": "Teslaを優先しすぎると、夕方Targetへ届かない可能性があります。Autoまたは蓄電池優先寄りの制御を推奨します。",
                "reasons": reasons
            }

        return {
            "active": True,
            "level": "success",
            "title": "Solar Priorityで運用可能です",
            "message": "solar window end以降の太陽光でAfternoon Targetへ回復できる見込みです。",
            "reasons": [
                f"solar window end以降の太陽光余力 {afternoon.get('solar_for_battery_kwh', 0):.1f}kWh / "
                f"蓄電池必要量 {afternoon.get('battery_needed_kwh', 0):.1f}kWh"
            ]
        }
    except Exception as e:
        return {
            "active": True,
            "level": "warning",
            "title": "Solar Priority警告を計算できません",
            "message": str(e),
            "reasons": []
        }





def get_effective_battery_mode(status_obj=None):
    """
    蓄電池制御サーバーの状態表現ゆれを吸収する。

    重要:
      status は API成否(success/ok/error) としても使われがちな名前なので、
      mode / intended_mode / current_mode を優先し、auto/charge/standby 等の
      有効なモード値だけを採用する。
    """
    valid_modes = {"auto", "charge", "standby", "stop", "stopped", "unknown"}
    aliases = {
        "charging": "charge",
        "discharge": "auto",
        "discharging": "auto",
        "idle": "standby",
        "hold": "standby",
    }

    try:
        s = status_obj or g_ctrl_status or {}
        if not isinstance(s, dict):
            return "unknown"

        for key in ["intended_mode", "mode", "current_mode", "battery_mode", "status"]:
            raw = s.get(key)
            if raw is None:
                continue
            mode = str(raw).strip().lower()
            mode = aliases.get(mode, mode)
            if mode in valid_modes:
                return "stop" if mode == "stopped" else mode

        return "unknown"
    except Exception:
        return "unknown"

def normalize_ctrl_status(status_obj):
    """
    /api/status の返却をREAD側で扱いやすい形に正規化する。
    """
    if not isinstance(status_obj, dict):
        return {"status": "unknown", "night_threshold": 30, "day_threshold": 30}

    normalized = dict(status_obj)
    mode = get_effective_battery_mode(normalized)
    if mode != "unknown":
        normalized["status"] = mode

    if "night_threshold" not in normalized:
        normalized["night_threshold"] = g_ctrl_status.get("night_threshold", 30)
    if "day_threshold" not in normalized:
        normalized["day_threshold"] = g_ctrl_status.get("day_threshold", 30)

    return normalized



def calc_tesla_amps_for_grid_target(pure_consumption, target_grid_w=None):
    """
    蓄電池予約枠を取らず、家の純消費 + Tesla が指定買電目安に収まるようTesla Aを計算する。
    solar-assist window early phaseの「Tesla最大化。ただしブレーカー安全範囲」のために使う。
    """
    try:
        max_amps = int(sys_settings.get("MAX_CHARGE_AMPS", 32))
        min_amps = int(settings.get("solar_priority_min_amps", 5))
        target_grid_w = float(target_grid_w if target_grid_w is not None else settings.get("day_early_tesla_target_grid_w", 6400))
        solar = float(state.get("solar", 0) or 0)
        pure = float(pure_consumption or 0)
        available_w = target_grid_w + solar - pure
        return max(min_amps, min(max_amps, int(math.floor(max(0, available_w) / 200.0))))
    except Exception:
        return int(settings.get("solar_priority_min_amps", 5))


def cheap_window_grid_maximize_active(now=None):
    """安価時間帯に買電6.4kW直下を狙う最大化制御が有効か。"""
    now = now or datetime.now(JST)
    try:
        if not bool(settings.get("tesla_grid_maximize_enabled", True)):
            return False
        if bool(settings.get("tesla_grid_max_only_cheap_window", True)) and not is_cheap_charge_window(now):
            return False
        return True
    except Exception:
        return False


def get_cheap_window_grid_targets(now=None):
    """
    安価充電時間帯のTesla最大化ターゲットを返す。

    grid_limit_w:
      超えたくない上限。通常は契約/実運用上限の6.4kW。
    grid_target_w:
      実際に狙う買電W。少し余白を取って grid_limit - headroom。
    warning_limit_w:
      ここを15秒超えたら1A以上下げる。
    critical_limit_w:
      ここを超えたら即時下げる。
    """
    try:
        grid_limit_w = float(settings.get("tesla_grid_max_target_w", sys_settings.get("GRID_LIMIT_W", 6400)) or 6400)
        headroom_w = max(0.0, float(settings.get("tesla_grid_max_headroom_w", 100) or 0))
        warning_margin_w = max(0.0, float(settings.get("tesla_grid_max_warning_margin_w", 50) or 0))
        critical_over_w = float(settings.get("tesla_grid_max_critical_over_w", 0) or 0)
        return {
            "grid_limit_w": grid_limit_w,
            "grid_target_w": max(0.0, grid_limit_w - headroom_w),
            "warning_limit_w": max(0.0, grid_limit_w - warning_margin_w),
            "critical_limit_w": grid_limit_w + critical_over_w,
        }
    except Exception:
        return {
            "grid_limit_w": float(sys_settings.get("GRID_LIMIT_W", 6400)),
            "grid_target_w": float(sys_settings.get("GRID_LIMIT_W", 6400)) - 100,
            "warning_limit_w": float(sys_settings.get("GRID_LIMIT_W", 6400)) - 50,
            "critical_limit_w": float(sys_settings.get("GRID_LIMIT_W", 6400)),
        }


def maybe_raise_tesla_amps_to_grid_limit(current_grid_w, current_amps, now=None, reason=""):
    """
    Wall Connector実測Aを基準に、買電6.4kW直下までTesla電流を上げる。

    下げる制御はブレーカー保護で即時行う。
    ここでは「まだ余白がある時だけ」ゆっくり上げる。
    """
    now = now or datetime.now(JST)
    try:
        if not bool(settings.get("tesla_grid_max_feedback_enabled", True)):
            return False
        if not cheap_window_grid_maximize_active(now):
            return False
        if not can_control_tesla_now():
            return False
        if not wall_connector_indicates_charging_load():
            return False

        max_amps = int(sys_settings.get("MAX_CHARGE_AMPS", 32))
        min_amps = int(settings.get("solar_priority_min_amps", 5))
        current_amps = int(current_amps or get_tesla_control_current_amps() or min_amps)
        if current_amps >= max_amps:
            return False

        targets = get_cheap_window_grid_targets(now)
        target_w = float(targets.get("grid_target_w", 6300))
        margin_w = max(0.0, float(settings.get("tesla_grid_max_increase_margin_w", 250) or 0))
        headroom_w = target_w - float(current_grid_w or 0)
        if headroom_w < margin_w:
            return False

        # 1A ≒ 200W。余白が大きくても一度に上げすぎず、Wall 5秒更新で追従する。
        step_a = max(1, int(math.floor(headroom_w / 200.0)))
        max_step_a = max(1, int(settings.get("tesla_grid_max_max_step_a", 3) or 3)) if "tesla_grid_max_max_step_a" in settings else 3
        step_a = min(step_a, max_step_a)
        target_amps = max(min_amps, min(max_amps, current_amps + step_a))
        if target_amps <= current_amps:
            return False

        return apply_tesla_amp_target(
            target_amps,
            current_amps,
            now,
            reason or f"Grid Maximize: 買電 {int(current_grid_w)}W / 目標 {int(target_w)}W"
        )
    except Exception as e:
        add_debug("Tesla Grid Maximize error", str(e), throttle_sec=300)
        return False


def calc_tesla_amps_for_solar_recovery(pure_consumption, assist_w=None):
    """
    買電BoostしないSolar Recovery用のTesla A目安。
    太陽光余剰 + 少量assistだけで決める。
    solar-assist window early phaseでも「ソーラーだけで十分なら買わない」を実現する。
    """
    try:
        min_amps = int(settings.get("solar_priority_min_amps", 5))
        max_amps = int(sys_settings.get("MAX_CHARGE_AMPS", 32))
        if assist_w is None:
            assist_w = float(settings.get("solar_recovery_grid_assist_w", 100) or 0)
        solar = float(state.get("solar", 0) or 0)
        pure = float(pure_consumption or 0)
        surplus_w = solar - pure + float(assist_w or 0)
        return max(min_amps, min(max_amps, int(math.floor(max(0.0, surplus_w) / 200.0))))
    except Exception:
        return int(settings.get("solar_priority_min_amps", 5))


def estimate_tesla_finish_delay_hours(is_charging=False):
    """Tesla充電完了までの概算時間[h]。time_to_fullが0でも充電中なら短い猶予を置く。"""
    try:
        if not is_charging:
            return 0.0
        ttf = float(tesla_state.get("time_to_full", 0) or 0)
        if ttf > 0:
            return ttf
        return float(settings.get("day_tesla_finish_grace_min", 10)) / 60.0
    except Exception:
        return float(settings.get("day_tesla_finish_grace_min", 10)) / 60.0


def can_battery_reach_target_by_solar_window_after_tesla(now, target_soc, is_charging=False):
    """
    Teslaを先に完了させても、solar window endまでに蓄電池が目標SOCへ届く見込みかをざっくり判定する。
    蓄電池の入力上限5kW前提。余裕係数で少し保守的に見る。
    """
    try:
        soc = float(state.get("soc", 0) or 0)
        target_soc = float(target_soc or 0)
        if target_soc <= 0 or soc >= target_soc:
            return True

        batt_max_wh = float(sys_settings.get("BATT_MAX_WH", 12600) or 12600)
        need_wh = max(0.0, (target_soc - soc) / 100.0 * batt_max_wh)
        hours_left = hours_until_window_end("solar_assist_charge_window", now, minimum=0.0)
        tesla_delay_h = min(hours_left, estimate_tesla_finish_delay_hours(is_charging))
        battery_hours = max(0.0, hours_left - tesla_delay_h)
        eff = float(settings.get("day_battery_feasible_efficiency", 0.90) or 0.90)
        possible_wh = 5000.0 * battery_hours * eff
        return possible_wh >= need_wh
    except Exception:
        return False


def should_hold_tesla_first(now, is_charging, afternoon=None):
    """
    solar assist windowにTeslaを優先してよいか。
    - solar-assist window early phaseはTesla最大化を優先
    - それ以降も、Teslaがすでに自宅充電中で、蓄電池目標がsolar window endまでに間に合うならTesla完了まで優先
    """
    try:
        if not is_charging:
            return False, ""
        if not bool(settings.get("day_tesla_hold_until_charge_done", True)):
            return False, ""

        early_until = int(settings.get("day_early_tesla_priority_until_minute", 10))
        if is_solar_window_early_phase(now):
            return True, f"solar-assist windowのearly phase（開始から{early_until}分）は蓄電池を触らず、Teslaをブレーカー安全範囲で最大化します。"

        afternoon = afternoon or {}
        target_soc = get_battery_first_target_soc(afternoon)
        if can_battery_reach_target_by_solar_window_after_tesla(now, target_soc, is_charging=True):
            return True, f"Teslaが先に自宅充電中で、蓄電池Target {target_soc}% はsolar window endまでに確保可能な見込みのため、Tesla完了まで優先します。"

        return False, ""
    except Exception as e:
        return False, f"Tesla優先判定エラー: {e}"



def minutes_until_solar_window(now):
    """solar_window_endまでの残り分。solar assist window以外でも安全に0以上を返す。"""
    try:
        return max(0.0, minutes_until_solar_window_end(now))
    except Exception:
        return 0.0


def estimate_battery_minutes_to_target_soc(target_soc):
    """蓄電池を5kW相当で充電した場合に、target_socまで必要な分数。"""
    try:
        soc = float(state.get("soc", 0) or 0)
        target_soc = float(target_soc or 0)
        missing_soc = max(0.0, target_soc - soc)
        soc_per_h = max(1.0, float(settings.get("battery_full_charge_soc_per_hour", 40.0) or 40.0))
        return (missing_soc / soc_per_h) * 60.0
    except Exception:
        return 999.0


def estimate_tesla_minutes_for_missing_km(missing_km, km_per_hour):
    """Tesla不足kmを指定回復速度で埋めるのに必要な分数。"""
    try:
        missing_km = max(0.0, float(missing_km or 0))
        km_per_hour = max(0.1, float(km_per_hour or 0.1))
        return (missing_km / km_per_hour) * 60.0
    except Exception:
        return 999.0


def estimate_forecast_solar_surplus_kwh_until_solar_window(now, pure_consumption):
    """
    今日の時間別予測から、現在時刻〜solar window endまでの太陽光余剰kWhをざっくり推定する。
    家庭純消費を一定として差し引く。予測が無い場合は0。
    """
    try:
        start_hour = int(now.hour)
        solar_end_hour = int(get_window_end_hour_float("solar_assist_charge_window", now, fallback=now.hour))
        solar_kwh = forecast_kwh_between(start_hour, solar_end_hour)
        hours = hours_until_window_end("solar_assist_charge_window", now, minimum=0.0)
        house_kwh = max(0.0, float(pure_consumption or 0) / 1000.0 * hours)
        return max(0.0, solar_kwh - house_kwh)
    except Exception:
        return 0.0


def get_battery_first_target_soc_safe(afternoon=None):
    """get_battery_first_target_socが未定義/例外でも落とさないラッパー。"""
    try:
        return int(get_battery_first_target_soc(afternoon))
    except Exception:
        return int(settings.get("afternoon_target_soc", 70))


def calculate_day_energy_schedule(now, pure_consumption, tesla_missing_km, afternoon=None, is_charging=False):
    """
    solar assist windowの時間配分型スケジューラ。

    方針:
      - ブレーカー保護は別段で最優先。ここでは優先順位だけ決める。
      - 蓄電池SOCが最低保護SOC未満ならTesla優先しない。
      - Tesla不足が小さく、太陽光5A相当でsolar window endまでに戻せるならSolar Recovery。
      - Teslaを太く先行しても、蓄電池Targetがsolar window endまでに間に合うならTesla Boost。
      - 蓄電池が時間的に間に合わないならBattery First。
    """
    afternoon = afternoon or estimate_afternoon_recovery(now, pure_consumption)
    minutes_left = minutes_until_solar_window(now)
    soc = float(state.get("soc", 0) or 0)
    target_soc = get_battery_first_target_soc_safe(afternoon)

    min_batt_soc = float(settings.get("day_tesla_priority_min_battery_soc", 40) or 40)
    margin_min = float(settings.get("day_time_split_margin_min", 10) or 10)
    battery_min = estimate_battery_minutes_to_target_soc(target_soc)
    full_kmh = float(settings.get("tesla_full_power_km_per_hour", 46.0) or 46.0)
    min_kmh = float(settings.get("tesla_min_amp_km_per_hour", 6.0) or 6.0)
    tesla_boost_min = estimate_tesla_minutes_for_missing_km(tesla_missing_km, full_kmh)
    tesla_solar_min = estimate_tesla_minutes_for_missing_km(tesla_missing_km, min_kmh)
    solar_recovery_max_km = float(settings.get("solar_recovery_max_missing_km", 15) or 15)
    boost_threshold_km = float(settings.get("day_boost_grid_threshold_km", 20) or 20)
    current_surplus_w = float(state.get("solar", 0) or 0) - float(pure_consumption or 0)
    solar_recovery_min_surplus_w = float(settings.get("solar_recovery_min_surplus_w", 700) or 700)
    forecast_surplus_kwh = estimate_forecast_solar_surplus_kwh_until_solar_window(now, pure_consumption)

    detail = {
        "minutes_left": round(minutes_left, 1),
        "tesla_missing_km": round(float(tesla_missing_km or 0), 1),
        "battery_target_soc": int(target_soc),
        "battery_minutes": round(battery_min, 1),
        "tesla_boost_minutes": round(tesla_boost_min, 1),
        "tesla_solar_minutes": round(tesla_solar_min, 1),
        "battery_min_soc": min_batt_soc,
        "current_surplus_w": int(current_surplus_w),
        "forecast_surplus_kwh": round(forecast_surplus_kwh, 2),
    }

    if minutes_left <= 0:
        return {"mode": "none", "strategy": "none", "reason": "solar window end以降のためsolar assistスケジューラ対象外です。", "target_soc": target_soc, "detail": detail}

    if soc < min_batt_soc and target_soc > soc:
        return {
            "mode": "battery_first",
            "strategy": "battery_floor",
            "reason": f"蓄電池SOC {soc:.0f}% がTesla優先許可下限 {min_batt_soc:.0f}% 未満のため、まず蓄電池を守ります。",
            "target_soc": target_soc,
            "detail": detail,
        }

    if battery_min + margin_min > minutes_left:
        return {
            "mode": "battery_first",
            "strategy": "battery_deadline",
            "reason": f"蓄電池Target {target_soc}% まで約{battery_min:.0f}分必要で、solar window endまでの余裕が少ないため蓄電池を優先します。",
            "target_soc": target_soc,
            "detail": detail,
        }

    # Tesla不足がほぼ無い/開始しづらい領域では、Tesla startは狙わず蓄電池へ。
    start_min_km = float(settings.get("tesla_start_min_missing_km", 8) or 8)
    if not is_charging and float(tesla_missing_km or 0) < start_min_km:
        return {
            "mode": "battery_first",
            "strategy": "tesla_too_small_to_start",
            "reason": f"Tesla不足 {tesla_missing_km:.0f}km は開始要求を受けない可能性が高いため、蓄電池を優先します。",
            "target_soc": target_soc,
            "detail": detail,
        }

    solar_can_recover_by_time = tesla_solar_min + margin_min <= minutes_left
    solar_current_ok = current_surplus_w >= solar_recovery_min_surplus_w
    # 1kWhあたり約7.7km相当として、予測余剰でも小不足を戻せるか見る。
    solar_forecast_ok = (forecast_surplus_kwh * (full_kmh / 6.0)) >= float(tesla_missing_km or 0)

    if (
        is_charging
        and float(tesla_missing_km or 0) <= solar_recovery_max_km
        and solar_can_recover_by_time
        and (solar_current_ok or solar_forecast_ok)
    ):
        return {
            "mode": "tesla_first",
            "strategy": "solar_recovery",
            "reason": f"Tesla不足 {tesla_missing_km:.0f}km は小さく、太陽光5A相当でsolar window endまでに回復可能なため、買電BoostせずSolar Recoveryにします。",
            "target_soc": target_soc,
            "detail": detail,
        }

    if (
        is_charging
        and float(tesla_missing_km or 0) >= boost_threshold_km
        and tesla_boost_min + battery_min + margin_min <= minutes_left
    ):
        return {
            "mode": "tesla_first",
            "strategy": "tesla_boost_then_battery",
            "reason": f"Teslaを先に太く充電しても、Tesla約{tesla_boost_min:.0f}分 + 蓄電池約{battery_min:.0f}分でsolar window endまでに両方間に合う見込みです。",
            "target_soc": target_soc,
            "detail": detail,
        }

    if is_charging and bool(settings.get("day_tesla_hold_until_charge_done", True)):
        if can_battery_reach_target_by_solar_window_after_tesla(now, target_soc, is_charging=True):
            return {
                "mode": "tesla_first",
                "strategy": "hold_until_done",
                "reason": f"Teslaがすでに充電中で、蓄電池Target {target_soc}% はsolar window endまでに確保可能な見込みのため、Tesla完了まで優先します。",
                "target_soc": target_soc,
                "detail": detail,
            }

    return {
        "mode": "battery_first",
        "strategy": "battery_default",
        "reason": f"Tesla不足 {tesla_missing_km:.0f}km と蓄電池残り時間を比較し、現時点では蓄電池を優先します。",
        "target_soc": target_soc,
        "detail": detail,
    }

def get_day_priority_mode(now, pure_consumption, tesla_missing_km, afternoon=None, mutate=False, is_charging=False):
    """
    solar assist windowの優先順位を返す。

    mutate=False:
      表示/確認用。グローバルなラッチ状態を書き換えない。
    mutate=True:
      実制御用。Day Priorityラッチを更新する。

    これにより、UI表示や安全判定で「見ただけなのに方針が切り替わる」副作用を防ぐ。
    """
    global g_day_priority_state, g_day_priority_since, g_day_priority_reason
    global g_day_priority_target_soc, g_day_priority_last_switch

    hold_sec = int(settings.get("day_priority_hold_sec", 300))

    if not is_solar_assist_charge_window(now):
        if mutate:
            g_day_priority_state = "none"
            g_day_priority_since = None
            g_day_priority_reason = ""
            g_day_priority_target_soc = 0
        return {
            "mode": "none",
            "reason": "solar assist windowではありません。",
            "target_soc": 0,
            "locked": False,
            "next_release": "solar_window_end",
            "mutated": bool(mutate),
        }

    afternoon = afternoon or estimate_afternoon_recovery(now, pure_consumption)

    user_mode = settings.get("day_priority_mode", "auto")
    day_target = int(g_ctrl_status.get("day_threshold", forecast_data.get("predicted_soc_window_end", 50)) or 50)
    soc = int(state.get("soc", 0))
    release_margin = int(settings.get("day_priority_release_margin_percent", 1))
    day_high_threshold = int(settings.get("day_priority_battery_first_day_target_threshold", 90))
    tesla_first_km = float(settings.get("day_priority_tesla_first_missing_km", 30))

    required_soc = int(max(
        afternoon.get("required_pre_solar_window_soc", 0) or 0,
        day_target if day_target >= day_high_threshold else 0
    ))

    strategy = "manual"
    detail = {}
    if user_mode in ["battery_first", "tesla_first"]:
        desired = user_mode
        reason = "ユーザー設定で Battery First" if user_mode == "battery_first" else "ユーザー設定で Tesla First"
    else:
        if bool(settings.get("day_energy_scheduler_enabled", True)):
            schedule = calculate_day_energy_schedule(now, pure_consumption, tesla_missing_km, afternoon, is_charging=is_charging)
            desired = schedule.get("mode", "battery_first")
            reason = schedule.get("reason", "Day Energy Scheduler")
            strategy = schedule.get("strategy", "unknown")
            detail = schedule.get("detail", {})
            required_soc = int(schedule.get("target_soc", required_soc) or required_soc)
        else:
            hold_tesla, hold_reason = should_hold_tesla_first(now, is_charging, afternoon)
            if hold_tesla:
                desired = "tesla_first"
                reason = hold_reason
                strategy = "hold_until_done"
            elif day_target >= day_high_threshold:
                desired = "battery_first"
                reason = f"Day Target {day_target}% が高いため、蓄電池を先に設定SOCへ近づけます。"
                strategy = "battery_high_target"
            elif not afternoon.get("can_recover_after_solar_window", True):
                desired = "battery_first"
                reason = "Afternoon Target不足見込みのため、蓄電池を先に確保します。"
                strategy = "battery_afternoon_deficit"
            elif tesla_missing_km >= tesla_first_km:
                desired = "tesla_first"
                reason = f"Tesla不足 {tesla_missing_km:.0f}km が大きいため、Teslaを先に充電します。"
                strategy = "tesla_missing_large"
            else:
                desired = "battery_first"
                reason = f"Tesla不足 {tesla_missing_km:.0f}km は小さいため、蓄電池を優先します。"
                strategy = "battery_tesla_small"

    # 現在のラッチ状態を尊重した切替条件。
    current_state = g_day_priority_state
    current_since = g_day_priority_since
    current_reason = g_day_priority_reason
    current_target = g_day_priority_target_soc

    if current_state == "battery_first":
        if required_soc > 0 and soc >= max(0, required_soc - release_margin):
            desired = "tesla_first"
            reason = f"蓄電池が必要SOC {required_soc}% 付近に到達したため、残り時間はTeslaへ回します。"
    elif current_state == "tesla_first":
        hold_tesla, hold_reason = should_hold_tesla_first(now, is_charging, afternoon)
        if hold_tesla:
            desired = "tesla_first"
            reason = hold_reason
        elif (not is_charging) and tesla_missing_km <= float(settings.get("proposal_tesla_small_km", 10)):
            desired = "battery_first"
            reason = f"Tesla充電が完了/停止し、Tesla不足が {tesla_missing_km:.0f}km まで小さくなったため、蓄電池を優先します。"
            strategy = "tesla_done"

    if current_state in ["battery_first", "tesla_first"] and current_since is not None:
        elapsed = (now - current_since).total_seconds()
        if desired != current_state and elapsed < hold_sec:
            return {
                "mode": current_state,
                "reason": current_reason + f"（最低保持中: 残り約{int(hold_sec - elapsed)}秒）",
                "target_soc": current_target,
                "locked": True,
                "next_release": (current_since + timedelta(seconds=hold_sec)).strftime("%H:%M:%S"),
                "mutated": False,
                "strategy": "locked",
                "detail": detail,
            }

    if mutate and desired != g_day_priority_state:
        g_day_priority_state = desired
        g_day_priority_since = now
        g_day_priority_last_switch = now
        g_day_priority_reason = reason
        g_day_priority_target_soc = required_soc
        current_state = g_day_priority_state
        current_since = g_day_priority_since
        current_reason = g_day_priority_reason
        current_target = g_day_priority_target_soc
    elif mutate:
        # 同じモード継続時も理由と目標SOCは最新化する。
        g_day_priority_reason = reason
        g_day_priority_target_soc = required_soc
        current_state = g_day_priority_state
        current_since = g_day_priority_since
        current_reason = g_day_priority_reason
        current_target = g_day_priority_target_soc
    else:
        # プレビュー時はラッチが無ければ計算結果を返す。既存ラッチがあればそれを優先。
        if current_state not in ["battery_first", "tesla_first"]:
            current_state = desired
            current_since = now
            current_reason = reason
            current_target = required_soc

    return {
        "mode": current_state,
        "reason": current_reason,
        "target_soc": current_target,
        "locked": current_state in ["battery_first", "tesla_first"],
        "next_release": (current_since + timedelta(seconds=hold_sec)).strftime("%H:%M:%S") if current_since else "solar_window_end",
        "mutated": bool(mutate),
    }

def calc_tesla_amps_with_battery_reserve(total_consumption, reserve_w):
    """
    蓄電池予約枠を残したうえで、Teslaへ渡せるAを計算する。
    Battery First時にTeslaがグリッド枠を食い尽くさないようにする。
    """
    min_amps = int(settings.get("battery_first_min_tesla_amps", 5))
    max_amps = int(sys_settings.get("MAX_CHARGE_AMPS", 32))

    safe_grid_w = float(sys_settings.get("TARGET_SAFE_POWER_W", 6000))
    pure = get_pure_home_consumption_w(total_consumption)
    solar = float(state.get("solar", 0))

    # Teslaに使える電力 = 安全買電枠 + 太陽光 - 家庭純消費 - 蓄電池予約枠
    available_w = safe_grid_w + solar - pure - float(reserve_w)
    target_amps = int(max(min_amps, min(max_amps, available_w / 200.0)))
    return target_amps


def build_tomorrow_operation_summary():
    """
    明日の動きの概略を表示するためのサマリ。
    低発電提案とsolar assist優先順位をつなげて説明する。
    """
    try:
        tomorrow_total = float(forecast_data.get("tomorrow_total", 0) or 0)
        low_th = float(settings.get("low_solar_threshold_kwh", 5.0))
        storm_th = float(settings.get("storm_solar_threshold_kwh", 3.0))

        if tomorrow_total <= 0:
            level = "warning"
            title = "明日の発電予測を取得中"
            policy = "予測取得後にNight/Day Target提案を更新します。"
        elif tomorrow_total < storm_th:
            level = "critical"
            title = "明日は荒天寄り"
            policy = "Night Target高め、DayはBattery First寄りを推奨します。"
        elif tomorrow_total < low_th:
            level = "warning"
            title = "明日は低発電日寄り"
            policy = "蓄電池を優先し、Teslaは不足が大きい時だけ優先します。"
        else:
            level = "success"
            title = "明日は通常〜良発電見込み"
            policy = "通常設定で運用し、昼は状況によりBattery/Teslaを選択します。"

        return {
            "title": title,
            "level": level,
            "forecast_date": forecast_data.get("tomorrow_date", "--/--"),
            "tomorrow_total_kwh": round(tomorrow_total, 2),
            "policy": policy,
            "recommended_day_priority": "battery_first" if tomorrow_total < low_th else "auto",
            "notes": [
                f"明日発電予測: {tomorrow_total:.1f}kWh",
                f"低発電判定: {low_th:.1f}kWh未満",
                "solar assist windowはBattery First / Tesla Firstのどちらかを固定して制御します。",
            ],
        }
    except Exception as e:
        return {
            "title": "明日の動きサマリ計算エラー",
            "level": "warning",
            "forecast_date": "--/--",
            "tomorrow_total_kwh": 0,
            "policy": str(e),
            "recommended_day_priority": "auto",
            "notes": [str(e)],
        }



def get_battery_first_target_soc(afternoon=None):
    """
    Battery First中に目指すSOC。
    Day Targetが高い場合はDay Targetを強いユーザー意思として優先。
    それ以外はAfternoon Targetに必要なpre_solar_window SOCを使う。
    """
    afternoon = afternoon or {}
    day_target = int(g_ctrl_status.get("day_threshold", forecast_data.get("predicted_soc_window_end", 50)) or 50)
    high_th = int(settings.get("day_priority_battery_first_day_target_threshold", 90))
    required_pre_solar_window = int(afternoon.get("required_pre_solar_window_soc", 0) or 0)

    if day_target >= high_th:
        return day_target

    return max(required_pre_solar_window, int(settings.get("afternoon_target_soc", 70)))


def enforce_day_priority_mode(now, total_consumption, pure_consumption, current_amps, is_charging, tesla_missing_km, afternoon=None):
    global g_last_tesla_start_time
    """
    solar assist windowのDay Priorityを実制御へ強制適用する。
    戻り値 True の場合、この周期のsolar assist制御はここで完了し、旧ロジックへ流さない。

    重要:
      Battery First中に旧ロジックが「Tesla充電中だからStandby」と判断すると、
      蓄電池Chargeが止まってしまうため、ここでreturnして後段を遮断する。
    """
    if not is_solar_assist_charge_window(now):
        return False

    afternoon = afternoon or estimate_afternoon_recovery(now, pure_consumption)
    day_priority = get_day_priority_mode(
        now,
        pure_consumption,
        tesla_missing_km,
        afternoon,
        mutate=True,
        is_charging=is_charging
    )

    mode = day_priority.get("mode", "none")
    soc = int(state.get("soc", 0))
    release_margin = int(settings.get("day_priority_release_margin_percent", 1))
    target_soc = get_battery_first_target_soc(afternoon)

    if mode == "battery_first":
        if soc < max(0, target_soc - release_margin):
            # Battery First中は必ず蓄電池Chargeを維持する。
            request_battery_command("charge")

            reserve_w = min(
                float(settings.get("battery_first_reserve_max_w", 3600)),
                max(
                    float(settings.get("battery_first_reserve_min_w", 1800)),
                    float(afternoon.get("battery_needed_kwh", 0) or 0) * 1000
                )
            )

            # Day Targetが高い時は強めに蓄電池枠を確保。
            day_target = int(g_ctrl_status.get("day_threshold", 50) or 50)
            if day_target >= int(settings.get("day_priority_battery_first_day_target_threshold", 90)):
                reserve_w = float(settings.get("battery_first_reserve_max_w", 3600))

            target_a = calc_tesla_amps_with_battery_reserve(total_consumption, reserve_w)

            tesla_auto_allowed = settings.get("tesla_auto_mode", True) and settings.get("tesla_charge_mode", "auto") != "manual"
            if is_charging and tesla_auto_allowed:
                apply_tesla_amp_target(
                    target_a,
                    current_amps,
                    now,
                    reason=f"Battery First: battery target {target_soc}% reserve {int(reserve_w)}W"
                )
            elif can_send_tesla_start() and target_a >= int(settings.get("battery_first_min_tesla_amps", 5)):
                if request_tesla_command("tesla_start"):
                    g_last_tesla_start_time = now
                    add_log("Battery First中Tesla開始", f"蓄電池予約枠を残してTeslaを開始します: {target_a}A目安", "info")

            return True

        # Battery First目標に到達。ここからTeslaへ回してよい。
        if get_effective_battery_mode(g_ctrl_status) != "auto" and soc < 100:
            if request_battery_command("auto"):
                add_log("Battery First完了", f"蓄電池が目標SOC {target_soc}% 付近に到達。Autoへ戻し、残り時間はTeslaへ回します", "success")

        return False

    if mode == "tesla_first":
        strategy = day_priority.get("strategy", "tesla_first")
        tesla_auto_allowed = settings.get("tesla_auto_mode", True) and settings.get("tesla_charge_mode", "auto") != "manual"

        # Tesla First中の蓄電池扱い。
        # boost系は蓄電池からTeslaへ流れないようStandby寄せ。
        # solar_recoveryは余剰を蓄電池へ吸わせたいので、蓄電池が放電している時だけStandby。
        if is_charging:
            batt_mode = get_effective_battery_mode(g_ctrl_status)
            batt_w = float(state.get("battery", 0) or 0)
            if strategy == "solar_recovery":
                if batt_w > 100 and batt_mode != "standby":
                    request_battery_command("standby")
                    add_log("Solar Recovery", "蓄電池がTesla側へ放電しているため、一時Standbyで吸い出しを防ぎます", "info")
                elif batt_w <= 100 and batt_mode == "standby" and state.get("soc", 0) < 100:
                    request_battery_command("auto")
            else:
                if batt_mode != "standby":
                    request_battery_command("standby")
                    add_log("Tesla First", "Tesla優先中のため、蓄電池をStandbyにして吸い出しを防ぎます", "info")

        if not tesla_auto_allowed:
            return True

        if is_charging:
            if strategy == "solar_recovery":
                # 買電Boostしない。太陽光余剰 + 少量assistの範囲で最低A〜可変。
                target_a = calc_tesla_amps_for_solar_recovery(pure_consumption)
                apply_tesla_amp_target(target_a, current_amps, now, "Solar Recovery: 太陽光回復優先")
                return True

            # boost/hold系はブレーカー安全範囲内でTeslaを太く使う。
            target_a = calc_tesla_amps_for_grid_target(
                pure_consumption,
                target_grid_w=float(settings.get("day_early_tesla_target_grid_w", 6400))
            )
            apply_tesla_amp_target(target_a, current_amps, now, f"Tesla First: {strategy}")
            return True

        # Teslaが停止中の場合、小不足ではstartを投げない。十分不足している場合のみ開始候補。
        if can_send_tesla_start(min_missing_km=float(settings.get("tesla_start_min_missing_km", 8))):
            target_a = calc_tesla_amps_for_grid_target(
                pure_consumption,
                target_grid_w=float(settings.get("day_early_tesla_target_grid_w", 6400))
            )
            if request_tesla_command("tesla_start"):
                g_last_tesla_start_time = now
                add_log("Tesla First開始", f"不足 {tesla_missing_km:.0f}km のためTesla開始要求。目安 {target_a}A", "info")
                request_tesla_amps_change(target_a, reason="tesla_first_start")
            return True

        return True

    return False



def is_day_battery_first_required(now, pure_consumption):
    """
    solar assist windowにBattery Firstが必要かを表示用ではなく制御用に判定する。
    Flow Summaryと実制御のズレを避けるため、シンプルな条件で判定する。
    """
    if not is_solar_assist_charge_window(now):
        return False, "solar assist windowではありません", 0, None

    try:
        tesla_missing_km = 0
        if tesla_state.get("soc", 0) > 0 and tesla_state.get("soc", 0) < tesla_state.get("charge_limit", 100):
            target_range = tesla_state.get("max_range", 397.0) * (tesla_state.get("charge_limit", 100) / 100.0)
            tesla_missing_km = max(0, target_range - tesla_state.get("range", 0))

        afternoon = estimate_afternoon_recovery(now, pure_consumption)
        is_charging = tesla_amp_control_active()
        day_priority = get_day_priority_mode(now, pure_consumption, tesla_missing_km, afternoon, is_charging=is_charging)
        mode = day_priority.get("mode", "none")

        target_soc = get_battery_first_target_soc(afternoon)
        margin = int(settings.get("day_priority_release_margin_percent", 1))
        soc = int(state.get("soc", 0))

        if mode == "battery_first" and soc < max(0, target_soc - margin):
            return True, day_priority.get("reason", "Battery First"), target_soc, afternoon

        return False, day_priority.get("reason", ""), target_soc, afternoon

    except Exception as e:
        return False, f"Battery First判定エラー: {e}", 0, None



def ensure_battery_mode(action, reason="", force=False):
    """
    蓄電池モードを目的の状態に寄せる。

    通常は request_battery_command の重複抑止を尊重する。
    Battery Firstのような強い方針でも、毎周期POSTする必要はない。

    force=True は手動操作や本当に緊急の時だけ。
    """
    try:
        action = str(action).lower()
        current = get_effective_battery_mode(g_ctrl_status)

        # 既に目的モードなら送らない
        if current == action:
            return False

        # 通常はdedupeありで送る
        sent = request_battery_command(action, force=force)

        # 送信成功/送信済み扱いならローカル状態も寄せて次周期の再送を防ぐ
        if sent:
            g_ctrl_status["status"] = action
            g_ctrl_status["mode"] = action
            g_ctrl_status["intended_mode"] = action
            if reason:
                add_log("蓄電池モード要求", f"{action}: {reason}", "info")
        return sent
    except Exception as e:
        add_log("蓄電池モード要求エラー", f"{action}: {e}", "warning")
        return False


def force_battery_first_cycle(now, total_consumption, pure_consumption, current_amps, is_charging, reason, target_soc, afternoon):
    """
    Battery Firstをこの周期で強制適用する。
    成功/失敗に関わらず、この周期の後段ロジックへ流さないためTrueを返す。

    fix7:
      - 既にcharge中ならchargeコマンドを再送しない
      - Battery First維持ログは60秒に1回、または目標Aが変わった時だけ出す
    """
    global g_last_battery_first_log_time, g_last_battery_first_log_key

    if "g_last_battery_first_log_time" not in globals():
        g_last_battery_first_log_time = None
    if "g_last_battery_first_log_key" not in globals():
        g_last_battery_first_log_key = ""

    try:
        # Battery First中は蓄電池Chargeが絶対優先。
        # ただし、すでにchargeなら毎周期POSTしない。
        current_batt_mode = get_effective_battery_mode(g_ctrl_status)
        charge_ok = (current_batt_mode == "charge")
        if current_batt_mode != "charge":
            charge_ok = request_battery_command("charge", force=False)
        if not charge_ok and get_effective_battery_mode(g_ctrl_status) != "charge":
            add_log("Battery First注意", "蓄電池Charge要求が成功確認できません。後段ロジックは止めず、次周期で再評価します。", "warning")
            return False

        day_target = int(g_ctrl_status.get("day_threshold", 50) or 50)
        high_th = int(settings.get("day_priority_battery_first_day_target_threshold", 90))

        reserve_w = float(settings.get("battery_first_reserve_min_w", 1800))
        if afternoon:
            need_kw = float(afternoon.get("battery_needed_kwh", 0) or 0) * 1000
            reserve_w = max(reserve_w, need_kw)

        # Day Targetが高いときは最大予約枠を確保。
        if day_target >= high_th:
            reserve_w = float(settings.get("battery_first_reserve_max_w", 3600))

        reserve_w = min(float(settings.get("battery_first_reserve_max_w", 3600)), reserve_w)

        target_a = calc_tesla_amps_with_battery_reserve(total_consumption, reserve_w)

        tesla_auto_allowed = settings.get("tesla_auto_mode", True) and settings.get("tesla_charge_mode", "auto") != "manual"
        if is_charging and tesla_auto_allowed:
            apply_tesla_amp_target(
                target_a,
                current_amps,
                now,
                reason=f"Battery First強制: target {target_soc}% / reserve {int(reserve_w)}W"
            )

        tesla_send_note = "Tesla充電中: A変更対象" if (is_charging and tesla_auto_allowed) else ("Tesla手動/自動OFF: A変更送信なし" if is_charging else "Tesla未充電: A変更送信なし")

        # ログは頻繁に出しすぎない。
        # target_aは太陽光/負荷で細かく揺れるため、ログキーには含めない。
        # これにより「Battery First維持」ログの大量発生を抑える。
        log_key = f"{target_soc}:{int(reserve_w)}:{int(bool(is_charging))}:{reason[:32]}"
        should_log = False
        if g_last_battery_first_log_time is None:
            should_log = True
        elif log_key != g_last_battery_first_log_key:
            should_log = True
        elif (now - g_last_battery_first_log_time).total_seconds() >= 60:
            should_log = True

        if should_log:
            add_log(
                "Battery First維持",
                f"{reason} / 蓄電池Charge優先 / Target {target_soc}% / Tesla想定上限 {target_a}A / Reserve {int(reserve_w)}W / {tesla_send_note}",
                "info"
            )
            g_last_battery_first_log_time = now
            g_last_battery_first_log_key = log_key

        return True

    except Exception as e:
        add_log("Battery First強制エラー", str(e), "warning")
        return True


def build_control_flow_summary(now, pure_consumption, is_charging, tesla_missing_km):
    """
    Decision Flowを直感的に表示するためのサマリ。
    文章だけでなく、現在の方針・理由・設定値・次の待ち処理をまとめる。
    """
    try:
        afternoon = estimate_afternoon_recovery(now, pure_consumption)
        target_hour = int(settings.get("afternoon_target_hour", 17))
        tesla_mode = settings.get("tesla_charge_mode", "auto")
        battery_mode = get_effective_battery_mode(g_ctrl_status)
        soc = int(state.get("soc", 0))
        day_target = int(g_ctrl_status.get("day_threshold", forecast_data.get("predicted_soc_window_end", 50)) or 50)
        night_target = int(g_ctrl_status.get("night_threshold", settings.get("night_threshold", 25)) or 25)
        afternoon_target = int(settings.get("afternoon_target_soc", 70))
        grid_assist = int(settings.get("solar_priority_grid_assist_w", 200))
        is_day = is_solar_assist_charge_window(now)
        is_night = is_primary_charge_window(now)
        tesla_aux_status = build_tesla_aux_status(now)

        # 現在方針
        priority_info = get_day_priority_mode(now, pure_consumption, tesla_missing_km, afternoon) if is_day else {"mode": "none", "reason": ""}

        if is_night:
            phase = "night"
            phase_label = "primary安価時間帯"
            if soc < night_target:
                policy = "蓄電池をNight Targetまで充電"
                reason = f"現在SOC {soc}% が Night Target {night_target}% を下回っています。"
                next_action = f"{night_target + int(settings.get('night_target_hysteresis_percent', 3))}% 付近まで充電後、Standbyで保持します。"
            else:
                policy = "primarySOCを保持"
                reason = f"現在SOC {soc}% は Night Target {night_target}% 以上です。"
                next_action = "primary_window_endにAutoへ戻し、通常運用へ移行します。"

        elif is_day:
            phase = "day"
            phase_label = "solar assist window"
            if priority_info.get("mode") == "battery_first":
                policy = "Battery First: 蓄電池を先に充電"
                reason = priority_info.get("reason", "蓄電池を優先します。")
                next_action = f"蓄電池がBattery First目標SOC {get_battery_first_target_soc(afternoon)}% 付近に届くまで、Teslaは蓄電池予約枠を邪魔しないAに制限します。"
            elif priority_info.get("mode") == "tesla_first":
                policy = "Tesla First: Teslaを先に充電"
                reason = priority_info.get("reason", "Teslaを優先します。")
                next_action = "Tesla不足が小さくなったらBattery Firstへ切り替えます。"
            elif afternoon.get("can_recover_after_solar_window", False):
                if is_charging:
                    policy = "Tesla優先可能 / 蓄電池Standby寄り"
                    reason = "solar window end以降の太陽光でAfternoon Targetへ回復できる見込みがあり、Tesla充電中のため蓄電池吸い出しを抑えます。"
                    next_action = "Tesla充電終了後、蓄電池SOCが100%未満ならAutoへ戻して太陽光を吸収します。"
                else:
                    policy = "蓄電池Autoで太陽光吸収"
                    reason = "Teslaが充電していないため、Standby保持より太陽光を蓄電池へ入れる方が有利です。"
                    next_action = "solar_window_endにsolar assist制御終了。Auto状態を維持します。"
            else:
                policy = "蓄電池予約枠を優先"
                reason = (
                    f"Afternoon Target {afternoon_target}% へ自然回復できない見込みです。"
                    f" 不足見込み {afternoon.get('deficit_kwh', 0):.1f}kWh。"
                )
                next_action = f"solar window end必要SOC {afternoon.get('required_pre_solar_window_soc', day_target)}% を目安に、蓄電池Chargeを優先します。"

        else:
            phase = "normal"
            phase_label = "通常時間帯"
            if tesla_mode == "solar_priority":
                policy = "Solar Priority SmartCharge"
                reason = "solar window end以降はGrid Assistを使わず、太陽光余剰中心でTesla充電を調整します。"
                next_action = "余剰不足や蓄電池放電が継続した場合、Teslaを5A化または停止します。"
            else:
                policy = "通常Auto運用"
                reason = "蓄電池はAuto、Teslaは必要時のみ制御します。"
                next_action = "次の主要イベントは、次のprimary charge windowまたはsolar assist windowです。"

        # Tesla補機/空調電力・待機Aリセット状態を方針表示へ反映
        if tesla_aux_status.get("aux_power_active", False) and not is_day and not is_night:
            policy = "Tesla補機/空調電力を監視中"
            reason = tesla_aux_status.get("detail", reason)
            next_action = "通常充電ではないためSmartCharge開始を抑制し、ブレーカー保護対象として監視します。"
        elif tesla_aux_status.get("idle_safe_reset_pending", False) and not is_day and not is_night:
            policy = "Tesla待機Aリセット待ち"
            reason = tesla_aux_status.get("detail", reason)
            next_action = f"次周期でTesla A設定を安全値 {tesla_aux_status.get('idle_safe_amps', 5)}A へ戻します。"

        # 注意・警告
        alerts = []
        try:
            if priority_info.get("mode") == "battery_first" and get_effective_battery_mode(g_ctrl_status) != "charge" and int(state.get("soc", 0)) < get_battery_first_target_soc(afternoon):
                alerts.append("Battery First中ですが蓄電池がChargeではありません。次周期でChargeを強制します。")
        except Exception:
            pass
        if tesla_state.get("charging_state") in ["Charging", "Starting"] and not tesla_state.get("home_charging", False):
            alerts.append("Teslaは充電中ですが、自宅充電と確定できないためHEMS制御対象外です。")
        if battery_mode == "standby" and is_day and not is_charging and soc < 100:
            alerts.append("蓄電池がStandbyですがTesla停止中です。Auto復帰して太陽光吸収するべき状態です。")
        if tesla_aux_status.get("aux_power_active", False):
            alerts.append(tesla_aux_status.get("label", "Tesla補機/空調電力中") + "。" + tesla_aux_status.get("detail", ""))
        elif tesla_aux_status.get("idle_safe_reset_pending", False):
            alerts.append(tesla_aux_status.get("label", "Tesla待機Aリセット対象") + "。" + tesla_aux_status.get("detail", ""))
        if not afternoon.get("can_recover_after_solar_window", True):
            alerts.append("Afternoon Target不足見込みのため、Teslaより蓄電池を優先する場面があります。")

        settings_summary = {
            "Night Target": f"{night_target}%",
            "Day Target": f"{day_target}%",
            "Afternoon Target": f"{afternoon_target}% @ {target_hour}:00",
            "Tesla Mode": str(tesla_mode),
            "Grid Assist": f"{grid_assist}W",
            "Tesla Home": "OK" if tesla_state.get("control_allowed", False) else "制御不可",
            "Tesla A Cooldown": "安価45s/通常120s/大差30s",
            "Battery Mode": battery_mode,
            "Tesla Aux/Idle": tesla_aux_status.get("label", "--"),
            "Idle Safe A": f"{tesla_aux_status.get('idle_safe_amps', settings.get('tesla_idle_safe_amps', 5))}A",
        }

        metrics = {
            "battery_soc": f"{soc}%",
            "solar_w": f"{int(state.get('solar', 0))}W",
            "grid_w": f"{int(state.get('grid', 0))}W",
            "pure_consumption_w": f"{int(pure_consumption)}W",
            "tesla_missing_km": f"{int(tesla_missing_km)}km",
            "afternoon_deficit_kwh": f"{afternoon.get('deficit_kwh', 0):.1f}kWh",
            "required_pre_solar_window_soc": f"{afternoon.get('required_pre_solar_window_soc', day_target)}%",
            "day_priority_reason": get_day_priority_mode(now, pure_consumption, tesla_missing_km, afternoon).get("reason", ""),
            "battery_first_target_soc": f"{get_battery_first_target_soc(afternoon)}%",
            "tesla_aux_power": f"{tesla_aux_status.get('aux_power_w', 0)}W",
            "tesla_reported_amps": f"{tesla_aux_status.get('reported_amps', 0)}A",
            "tesla_planned_window": "Yes" if tesla_aux_status.get("planned_charge_window", False) else "No",
            "tesla_idle_reset": "Pending" if tesla_aux_status.get("idle_safe_reset_pending", False) else ("Allowed" if tesla_aux_status.get("idle_safe_reset_allowed", False) else "No"),
            "tesla_smartcharge_suppressed": "Yes" if tesla_aux_status.get("smartcharge_start_suppressed", False) else "No",
            "tesla_breaker_guard": "Yes" if tesla_aux_status.get("breaker_guard_target", False) else "No",
        }

        return {
            "phase": phase,
            "phase_label": phase_label,
            "policy": policy,
            "reason": reason,
            "next_action": next_action,
            "alerts": alerts,
            "settings_summary": settings_summary,
            "metrics": metrics,
            "afternoon": afternoon,
        }
    except Exception as e:
        return {
            "phase": "error",
            "phase_label": "Flow Summary Error",
            "policy": "計算エラー",
            "reason": str(e),
            "next_action": "ログを確認してください。",
            "alerts": [str(e)],
            "settings_summary": {},
            "metrics": {},
            "afternoon": {},
        }


def build_control_flow(now, pure_consumption, is_charging, tesla_missing_km):
    """固定時刻だけではなく、今後起きる見込みの制御イベントをUIへ返す。"""
    flow = []

    def add(time_label, title, detail, level="info", icon="●", action="監視"):
        flow.append({
            "time": time_label,
            "title": title,
            "detail": detail,
            "level": level,
            "icon": icon,
            "action": action
        })

    afternoon = estimate_afternoon_recovery(now, pure_consumption)
    target_soc = get_effective_day_target()
    tesla_mode = settings.get("tesla_charge_mode", "auto")
    heavy_charge_km = float(sys_settings.get("TESLA_HEAVY_CHARGE_KM", 30))
    tesla_aux_status = build_tesla_aux_status(now)

    now_detail = f"Battery {g_ctrl_status.get('status', 'unknown')} / Tesla {tesla_state.get('charging_state', 'Stopped')} / Mode {tesla_mode}"
    if tesla_aux_status.get("aux_power_active", False) or tesla_aux_status.get("idle_safe_reset_pending", False):
        now_detail += f" / {tesla_aux_status.get('label', '')}: {tesla_aux_status.get('aux_power_w', 0)}W"

    add(
        "Now",
        "現在の制御状態",
        now_detail,
        tesla_aux_status.get("level", "info") if (tesla_aux_status.get("aux_power_active", False) or tesla_aux_status.get("idle_safe_reset_pending", False)) else "info",
        "◎",
        "状態確認"
    )

    if tesla_aux_status.get("aux_power_active", False) or tesla_aux_status.get("idle_safe_reset_pending", False):
        add(
            "Now",
            tesla_aux_status.get("label", "Tesla補機/待機状態"),
            tesla_aux_status.get("detail", ""),
            tesla_aux_status.get("level", "warning"),
            "🚗",
            "補機/待機監視"
        )

    if not is_solar_assist_charge_window(now) or is_solar_window_early_phase(now):
        if tesla_missing_km >= heavy_charge_km:
            detail = "Tesla不足が大きい場合、solar-assist window early phaseは蓄電池を触らずTesla充電を優先します。"
            level = "warning" if tesla_mode == "solar_priority" and not afternoon.get("can_recover_after_solar_window", False) else "info"
        else:
            detail = "Tesla不足が小さければ、太陽光優先で控えめに充電します。"
            level = "info"
        add("solar_window_start", "solar assist window開始", detail, level, "⚡", "Tesla優先判定")

        add(
            "solar-assist + early phase",
            "蓄電池回復判定開始",
            "solar window end以降の太陽光でAfternoon Targetへ回復できるかを見て、蓄電池Chargeが必要か判断します。",
            "info",
            "🔋",
            "solar window後回復判定"
        )

    if is_solar_assist_charge_window(now) and not is_solar_window_early_phase(now):
        if afternoon.get("can_recover_after_solar_window", False):
            add(
                "〜solar_window_end",
                "Tesla優先可能",
                "solar window end以降の太陽光で蓄電池が回復できる見込みです。Tesla充電中は蓄電池Standby寄りで吸い出しを防ぎます。",
                "success",
                "🚗",
                "Tesla優先"
            )
        else:
            add(
                "〜solar_window_end",
                "蓄電池予約枠を確保",
                f"Afternoon Target不足見込みです。solar window end必要SOCは約 {afternoon.get('required_pre_solar_window_soc', target_soc)}%。Tesla電流は蓄電池枠を残して調整します。",
                "warning",
                "🔋",
                "Battery優先"
            )

    if not is_after_solar_window(now):
        add(
            "solar_window_end",
            "solar assist制御終了",
            "蓄電池をAutoへ戻します。solar window end以降は太陽光余剰を蓄電池へ自然に受けます。",
            "success",
            "↩",
            "Auto復帰"
        )
    elif is_window_end_recovery_due("solar_assist_charge_window", now, grace_minutes=60):
        add(
            "solar_window_end台",
            "Auto復帰確認",
            "solar assist制御で入れたStandbyが残っていれば解除します。",
            "success",
            "↩",
            "保険処理"
        )

    target_hour = int(settings.get("afternoon_target_hour", 17))
    if now.hour < target_hour:
        if tesla_mode == "solar_priority":
            add(
                "solar_window_end以降",
                "Solar Priority SmartCharge",
                "蓄電池はAuto。Teslaは発電余剰以下を基本に充電し、短時間の蓄電池放電はバッファとして許容します。",
                "info",
                "☀",
                "余剰追従"
            )
        else:
            add(
                "solar_window_end以降",
                "SmartCharge",
                "太陽光余剰が十分な場合にTesla充電を開始・調整します。",
                "info",
                "☀",
                "余剰追従"
            )

        if afternoon.get("can_recover_after_solar_window", False):
            add(
                f"{target_hour}:00",
                "Afternoon Target到達見込み",
                f"目標 {settings.get('afternoon_target_soc', 70)}% に届く見込みです。",
                "success",
                "✓",
                "Target確認"
            )
        else:
            add(
                f"{target_hour}:00",
                "Afternoon Target不足見込み",
                f"不足見込み {afternoon.get('deficit_kwh', 0):.1f}kWh。solar assist windowに蓄電池予約枠が必要です。",
                "warning",
                "!",
                "不足警告"
            )

    add(
        "primary_window_start",
        "primary安価時間帯",
        "Night Target未満なら蓄電池Charge。Tesla充電中かつ蓄電池十分ならStandbyで枠を空けます。",
        "info",
        "☾",
        "Night制御"
    )

    return flow[:10]



def build_tesla_api_payload():
    """UI/APIへ返すTesla状態。自宅充電判定も明示する。"""
    payload = dict(tesla_state)
    try:
        total = estimate_total_home_consumption_w()
        payload["home_charging"] = bool(is_tesla_home_charging(total))
        payload["home_charge_reason"] = tesla_state.get("home_charge_reason", "unknown")
        payload["geofence"] = tesla_state.get("geofence", "")
        payload["is_home"] = tesla_state.get("is_home", None)
        payload["control_allowed"] = bool(can_control_tesla_now())
        payload["charger_power"] = float(tesla_state.get("charger_power", tesla_state.get("power", 0)) or 0)
        payload["power"] = payload["charger_power"]  # existing UI compatibility
        aux_status = build_tesla_aux_status(datetime.now(JST))
        payload["aux_status"] = aux_status
        payload["aux_power_active"] = bool(aux_status.get("aux_power_active", False))
        payload["aux_power_kw"] = float(aux_status.get("aux_power_kw", 0.0))
        payload["planned_charge_window"] = bool(aux_status.get("planned_charge_window", False))
        payload["idle_safe_reset_allowed"] = bool(aux_status.get("idle_safe_reset_allowed", False))
        payload["idle_safe_reset_pending"] = bool(aux_status.get("idle_safe_reset_pending", False))
        payload["smartcharge_start_suppressed"] = bool(aux_status.get("smartcharge_start_suppressed", False))
        payload["breaker_guard_target"] = bool(aux_status.get("breaker_guard_target", False))
        payload["wall_connector"] = build_wall_connector_payload()
    except Exception as e:
        payload["home_charging"] = False
        payload["home_charge_reason"] = f"api_payload_error_{e}"
        payload["control_allowed"] = False
        payload["aux_status"] = {"active": False, "label": "Tesla補機/待機状態取得エラー", "detail": str(e)}
    return payload


@app.route('/api/data')
def get_data():
    now = datetime.now(JST)
    b = state["battery"]
    total_consumption = estimate_total_home_consumption_w()
    wall_payload_for_api = build_wall_connector_payload()
    try:
        tesla_wall_power_w = max(0.0, float(wall_payload_for_api.get("power_w") or 0.0))
    except Exception:
        tesla_wall_power_w = 0.0
    home_consumption_without_tesla = max(0.0, float(total_consumption) - tesla_wall_power_w)
    load_source_split = estimate_load_source_split(home_consumption_without_tesla, tesla_wall_power_w)
    tesla_range_dataset = build_tesla_range_dataset(wall_payload_for_api)
    # API表示・提案計算でも、Teslaが自宅充電中の時だけTesla電力を家庭純消費から差し引く。
    is_tesla_home_charging(total_consumption)
    pure_consumption = get_pure_home_consumption_w(total_consumption)
    
    batt_max_wh = float(sys_settings.get("BATT_MAX_WH", 12600))
    grid_limit_w = float(sys_settings.get("GRID_LIMIT_W", 6400))
    
    rem_wh = state["rem_wh"] if state["rem_wh"] > 0 else (state["soc"] / 100.0 * batt_max_wh)
    
    tesla_state["is_custom"] = False
    if tesla_state["actual_current"] > 0 and tesla_state["soc"] > 0 and tesla_state["charge_limit"] > tesla_state["soc"]:
        speed_kmh = get_real_charge_speed_kmh(tesla_state["actual_current"])
        if speed_kmh > 0:
            target_range = tesla_state["max_range"] * (tesla_state["charge_limit"] / 100.0)
            rem_km = target_range - tesla_state["range"]
            tesla_state["time_to_full"] = max(0, rem_km / speed_kmh)
            tesla_state["is_custom"] = True

    def predict_wh(target_h):
        target_time = now.replace(hour=target_h, minute=0, second=0, microsecond=0)
        if target_time <= now: target_time += timedelta(days=1)
        hours_diff = (target_time - now).total_seconds() / 3600
        
        battery_w = b 
        if battery_w > 0:
            if is_primary_charge_window(now):
                charge_end_time = get_window_end_datetime("primary_charge_window", now)
            elif is_solar_assist_charge_window(now):
                charge_end_time = get_window_end_datetime("solar_assist_charge_window", now)
            else:
                fallback_hour = int(settings.get("afternoon_target_hour", now.hour))
                charge_end_time = now.replace(hour=max(0, min(23, fallback_hour)), minute=0, second=0, microsecond=0)
                if charge_end_time <= now:
                    charge_end_time += timedelta(days=1)
            
            max_charge_duration = (charge_end_time - now).total_seconds() / 3600
            actual_charge_h = min(hours_diff, max_charge_duration)
            discharge_h = max(0, hours_diff - actual_charge_h)
            
            available_grid_w = grid_limit_w + state["solar"] - pure_consumption
            post_tesla_w = max(0, min(5000, available_grid_w))
            
            if tesla_state["charging_state"] == "Charging" and tesla_state["time_to_full"] > 0:
                t_full = tesla_state["time_to_full"]
                phase1_h = min(actual_charge_h, t_full)
                phase2_h = max(0, actual_charge_h - phase1_h)
                added_wh = (battery_w * phase1_h) + (post_tesla_w * phase2_h)
            else: added_wh = battery_w * actual_charge_h
                
            lost_wh = pure_consumption * discharge_h
            predicted_wh = rem_wh + added_wh - lost_wh
        else: predicted_wh = rem_wh + (battery_w * hours_diff)
        return max(0, min(batt_max_wh, predicted_wh))
    
    ctrl_status = {"status": "auto", "night_threshold": 30, "day_threshold": 30}
    try:
        ctrl_res = requests.get(post_control_url("/api/status"), timeout=2)
        if ctrl_res.status_code == 200:
            ctrl_status = normalize_ctrl_status(ctrl_res.json()); global g_ctrl_status; g_ctrl_status = ctrl_status
    except: pass 
        
    remaining_solar_kwh = 0
    for h_data in forecast_data.get("today_hourly", []):
        try:
            if int(h_data["time"].split(":")[0]) >= now.hour: remaining_solar_kwh += h_data["gen"]
        except: pass
        
    tesla_rem_kwh = 0
    if tesla_state["soc"] > 0 and tesla_state["soc"] < tesla_state["charge_limit"]:
        target_range = tesla_state["max_range"] * (tesla_state["charge_limit"] / 100.0)
        tesla_rem_kwh = (max(0, target_range - tesla_state["range"]) * 145) / 1000.0
        
    house_rem_kwh = (pure_consumption / 1000.0) * max(0, 17 - now.hour)
    batt_rem_kwh = max(0, (get_effective_day_target(now) - state["soc"]) / 100.0 * (batt_max_wh / 1000.0))
    
    total_needed_kwh = tesla_rem_kwh + house_rem_kwh + batt_rem_kwh
    
    current_forecast = forecast_data.copy()
    current_forecast["skip_day_boost"] = (remaining_solar_kwh >= total_needed_kwh and total_needed_kwh > 0)
    current_forecast["remaining_solar_kwh"] = round(remaining_solar_kwh, 1)
    current_forecast["total_needed_kwh"] = round(total_needed_kwh, 1)

    # --- JIT Analytics for UI Flow ---
    jit_info = {"active": False, "target_soc": 0, "req_wh": 0, "hours_left": 0, "batt_req_w": 0, "time_to_target_h": 0}
    is_day = is_solar_assist_charge_window(now); is_night = is_primary_charge_window(now)
    
    if is_day or is_night:
        jit_info["active"] = True
        jit_info["target_soc"] = get_effective_day_target(now) if is_day else ctrl_status.get("night_threshold", 30)
        end_hour = get_window_end_hour_float("solar_assist_charge_window" if is_day else "primary_charge_window", now, fallback=(now.hour + now.minute / 60.0))
        jit_info["hours_left"] = max(0.0, end_hour - (now.hour + now.minute / 60.0))
        jit_info["req_wh"] = max(0, (jit_info["target_soc"] - state["soc"]) / 100.0 * batt_max_wh)
        
        if jit_info["req_wh"] > 0 and jit_info["hours_left"] > 0:
            jit_info["batt_req_w"] = min(5000.0, jit_info["req_wh"] / jit_info["hours_left"])
            actual_batt_in = state["battery"]
            if actual_batt_in > 100:
                jit_info["time_to_target_h"] = jit_info["req_wh"] / actual_batt_in
            else:
                jit_info["time_to_target_h"] = jit_info["req_wh"] / max(100, jit_info["batt_req_w"])
        else:
            jit_info["time_to_target_h"] = 0

    afternoon_info = estimate_afternoon_recovery(now, pure_consumption)

    solar_priority_warning = build_solar_priority_warning(now, pure_consumption)
    control_flow = build_control_flow(
        now,
        pure_consumption,
        tesla_state["charging_state"] in ["Charging", "Starting"],
        tesla_rem_kwh / 0.145 if tesla_rem_kwh > 0 else 0
    )
    low_solar_proposal = build_low_solar_proposal(now, pure_consumption)

    flow_summary = build_control_flow_summary(
        now,
        pure_consumption,
        tesla_state["charging_state"] in ["Charging", "Starting"] and tesla_state.get("home_charging", False),
        tesla_rem_kwh / 0.145 if tesla_rem_kwh > 0 else 0
    )

    tomorrow_operation = build_tomorrow_operation_summary()

    return jsonify({
        **state, "tesla": build_tesla_api_payload(), "tesla_aux_status": build_tesla_aux_status(now),
        "consumption": total_consumption, "pure_consumption": pure_consumption,
        "home_consumption_without_tesla": home_consumption_without_tesla,
        "tesla_wall_power_w": tesla_wall_power_w,
        "tesla_range_status": tesla_range_dataset,
        "tesla_soc_percent": tesla_range_dataset.get("soc_percent"),
        "tesla_range_km": tesla_range_dataset.get("range_km"),
        "tesla_max_range_km": tesla_range_dataset.get("max_range_km"),
        "tesla_charge_limit_percent": tesla_range_dataset.get("charge_limit_percent"),
        "tesla_target_range_km": tesla_range_dataset.get("target_range_km"),
        "tesla_missing_km": tesla_range_dataset.get("missing_km"),
        "tesla_range_data_source": tesla_range_dataset.get("data_source"),
        "load_source_split": load_source_split,
        "grid_cap": grid_limit_w - state["grid"],
        "read_only": g_read_only_mode,
        "predicts": [
            {"time": "primary_end", "soc": round((predict_wh(int(get_window_end_hour_float("primary_charge_window", now, fallback=now.hour)))/batt_max_wh)*100), "wh": round(predict_wh(int(get_window_end_hour_float("primary_charge_window", now, fallback=now.hour))))},
            {"time": "solar_start", "soc": round((predict_wh(now.hour)/batt_max_wh)*100), "wh": round(predict_wh(now.hour))},
            {"time": "solar_end", "soc": round((predict_wh(int(get_window_end_hour_float("solar_assist_charge_window", now, fallback=now.hour)))/batt_max_wh)*100), "wh": round(predict_wh(int(get_window_end_hour_float("solar_assist_charge_window", now, fallback=now.hour))))},
            {"time": "primary_start", "soc": round((predict_wh(now.hour)/batt_max_wh)*100), "wh": round(predict_wh(now.hour))}
        ],
        "rate": get_rate_info(), "forecast": current_forecast, "afternoon": afternoon_info,
        "wall_connector": wall_payload_for_api,
        "settings": settings, "sys_settings": sys_settings, "debug_logs": debug_logs,
        "ctrl": ctrl_status, "logs": app_logs, "jit": jit_info, "solar_priority_warning": solar_priority_warning, "control_flow": control_flow, "flow_summary": flow_summary, "low_solar_proposal": low_solar_proposal, "tomorrow_operation": tomorrow_operation
    })

@app.route('/api/logs/file')
def get_file_logs():
    now = datetime.now(JST)
    filename = f"SmartHEMS_log{now.strftime('%Y%m%d')}.txt"
    if not os.path.exists(filename): return jsonify({"status": "error", "content": "本日のログファイルはまだありません。"})
    try:
        with open(filename, "r", encoding="utf-8") as f: lines = f.readlines()
        return jsonify({"status": "success", "content": "".join(lines[-100:])})
    except Exception as e: return jsonify({"status": "error", "content": f"読み込みエラー: {e}"})

@app.route('/api/command', methods=['POST'])
def handle_command():
    global g_read_only_mode, g_manual_day_target_until, g_manual_day_target_value, g_ctrl_status
    data = request.json
    target, action, value = data.get('target'), data.get('action'), data.get('value')

    if target == 'settings':
        if action == 'set_read_only':
            g_read_only_mode = bool(value)
            status_txt = "ON (制御凍結)" if g_read_only_mode else "OFF (自動再開)"
            add_log("マニュアル操作", f"読み取り専用モードを {status_txt} に切り替えました", "warning" if g_read_only_mode else "info")
            return jsonify({"status": "success", "message": f"読み取り専用モードを {status_txt} にしました。"})
        elif action == 'set_day_auto':
            settings["day_auto_mode"] = bool(value)
            add_log("マニュアル操作", f"solar assistAI自動調整を {'ON' if value else 'OFF'} に変更", "info")
            if settings["day_auto_mode"] and not g_read_only_mode:
                try:
                    requests.post(post_control_url("/api/command"), json={"action": "thre_day", "value": str(get_effective_day_target())}, timeout=5)
                except:
                    pass
            return jsonify({"status": "success"})
        elif action == 'set_tesla_auto':
            settings["tesla_auto_mode"] = bool(value)
            add_log("マニュアル操作", f"テスラ自動制御を {'ON' if value else 'OFF'} に変更", "info")
            return jsonify({"status": "success"})
        elif action == 'set_tesla_min_soc':
            settings["tesla_min_soc"] = int(value)
            add_log("マニュアル操作", f"テスラ充電解禁ラインを {value}% に設定", "info")
            return jsonify({"status": "success"})
        elif action == 'set_debug_mode':
            settings["debug_mode"] = bool(value)
            add_log("マニュアル操作", f"デバッグログを {'ON' if value else 'OFF'} に変更", "info")
            add_debug("設定変更", f"debug_mode={settings['debug_mode']}", {"value": settings["debug_mode"]})
            return jsonify({"status": "success"})
        elif action == 'set_tesla_charge_mode':
            mode = str(value)
            if mode not in ["auto", "solar_priority", "manual"]:
                return jsonify({"status": "error", "message": "mode must be auto / solar_priority / manual"})
            settings["tesla_charge_mode"] = mode
            add_log("マニュアル操作", f"Tesla充電モードを {mode} に変更", "info")
            return jsonify({"status": "success"})
        elif action == 'set_solar_priority_grid_assist':
            settings["solar_priority_grid_assist_w"] = int(value)
            add_log("マニュアル操作", f"Solar Priorityの買電許容を {value}W に設定", "info")
            return jsonify({"status": "success"})
        elif action == 'set_afternoon_target_soc':
            settings["afternoon_target_soc"] = int(value)
            add_log("マニュアル操作", f"Afternoon Targetを {value}% に設定", "info")
            return jsonify({"status": "success"})
        elif action == 'set_afternoon_target_hour':
            settings["afternoon_target_hour"] = int(value)
            add_log("マニュアル操作", f"Afternoon Target Hourを {value}:00 に設定", "info")
            return jsonify({"status": "success"})
        elif action == 'set_afternoon_recovery_mode':
            settings["afternoon_recovery_mode"] = bool(value)
            add_log("マニュアル操作", f"solar window後回復判定を {'ON' if value else 'OFF'} に変更", "info")
            return jsonify({"status": "success"})
        elif action == 'set_low_solar_proposal_mode':
            settings["low_solar_proposal_mode"] = bool(value)
            add_log("マニュアル操作", f"低発電日提案モードを {'ON' if value else 'OFF'} に変更", "info")
            return jsonify({"status": "success"})
        elif action == 'dismiss_low_solar_proposal':
            proposal = build_low_solar_proposal(datetime.now(JST), get_pure_home_consumption_w())
            settings["proposal_dismissed_date"] = proposal.get("forecast_date", datetime.now(JST).strftime("%m/%d"))
            add_log("提案モード", f"低発電日提案を無視にしました: {settings['proposal_dismissed_date']}", "info")
            return jsonify({"status": "success"})
        elif action == 'apply_low_solar_proposal':
            proposal = build_low_solar_proposal(datetime.now(JST), get_pure_home_consumption_w())
            night_target = int((value or {}).get("night_target", proposal.get("recommended_night_target", 50))) if isinstance(value, dict) else int(proposal.get("recommended_night_target", 50))
            day_target = int((value or {}).get("day_target", proposal.get("recommended_day_target", 100))) if isinstance(value, dict) else int(proposal.get("recommended_day_target", 100))
            if g_read_only_mode:
                return jsonify({"status": "error", "message": "読み取り専用モード中は適用できません。"})
            try:
                requests.post(post_control_url("/api/command"), json={"action": "thre_night", "value": str(night_target)}, timeout=5)
                requests.post(post_control_url("/api/command"), json={"action": "thre_day", "value": str(day_target)}, timeout=5)
                g_ctrl_status["night_threshold"] = night_target
                g_ctrl_status["day_threshold"] = day_target
                forecast_data["predicted_soc_window_end"] = day_target

                now = datetime.now(JST)
                manual_until = get_window_end_datetime("solar_assist_charge_window", now) + timedelta(minutes=int(settings.get("manual_day_target_hold_minutes", 10)))
                if manual_until <= now:
                    manual_until = now + timedelta(hours=1)
                g_manual_day_target_value = day_target
                g_manual_day_target_until = manual_until

                add_log("提案モード適用", f"Night Target={night_target}% / Day Target={day_target}% を適用しました", "success")
                return jsonify({"status": "success", "proposal": proposal, "night_target": night_target, "day_target": day_target})
            except Exception as e:
                add_log("提案モード適用失敗", str(e), "warning")
                return jsonify({"status": "error", "message": str(e)})

    if target == 'tesla':
        if action == 'refresh_fleet_state':
            data = request_fleet_state_from_post(reason="manual_ui_refresh")
            if not data:
                return jsonify({"status": "error", "message": ev_wall_state.get("last_error") or "Fleet API取得に失敗しました。"})

            # 手動更新はSOC/km cacheを更新するのが主目的。
            # Wall負荷が出ている場合のみ、現在セッションの分類も最新化する。
            now_for_refresh = datetime.now(JST)
            if ev_wall_state.get("load_active") or float(ev_wall_state.get("power_w") or 0) > float(settings.get("wall_activity_min_w", 50)):
                cls = str(data.get("classification") or "unknown_ev_load")
                if cls not in ["charging", "charging_with_climate", "climate_only", "none"]:
                    cls = "unknown_ev_load"
                if cls == "none" and (ev_wall_state.get("load_active") or float(ev_wall_state.get("power_w") or 0) > 0):
                    cls = "unknown_ev_load"
                ev_wall_state["fleet_data"] = data
                ev_wall_state["fleet_last_fetch"] = now_for_refresh
                ev_wall_state["fleet_checked_for_session"] = True
                ev_wall_state["load_type"] = cls
                notify_post_tesla_charging_state(fleet_classification_is_charging(cls), reason="manual_ui_refresh")
                if cls == "charging":
                    start_tesla_wall_range_estimate(now_for_refresh)

            add_log("Tesla手動更新", "Fleet APIからSOC/kmを手動更新しました", "info")
            return jsonify({"status": "success", "message": "Tesla SOC/kmを更新しました。", "fleet": data, "range": build_tesla_range_dataset(build_wall_connector_payload())})

    if target == 'battery':
        if g_read_only_mode:
            return jsonify({"status": "error", "message": "読み取り専用モード中は送信できません。"})
        try:
            payload = {"action": action}
            if value is not None:
                payload["value"] = value

            # Day Targetを手動変更した場合、READ側の実効targetも即時更新し、configured hold windowまでAI上書きを抑制する。
            if action == "thre_day" and value is not None:
                manual_value = int(value)
                forecast_data["predicted_soc_window_end"] = manual_value
                g_ctrl_status["day_threshold"] = manual_value
                now = datetime.now(JST)
                manual_until = get_window_end_datetime("solar_assist_charge_window", now) + timedelta(minutes=int(settings.get("manual_day_target_hold_minutes", 10)))
                if manual_until <= now:
                    manual_until = now + timedelta(hours=1)
                g_manual_day_target_value = manual_value
                g_manual_day_target_until = manual_until
                add_log("手動Day Target", f"Day Targetを {manual_value}% に変更。{manual_until.strftime('%H:%M')}までAI上書きを抑制", "warning")

            # UIからTesla電流を明示指定した場合は、完全固定ではなくSolar Priorityへ切替。
            if action == "tesla" and value is not None:
                settings["tesla_charge_mode"] = "solar_priority"
                add_log("Tesla充電モード", f"手動電流指定 {value}A を受け、Solar Priorityへ切替", "info")

            res = requests.post(post_control_url("/api/command"), json=payload, timeout=5).json()
            if res.get("status") == "success":
                add_log("マニュアル操作", f"バッテリー/CTRL {action} を送信", "info")

                # 新target到達済みなら、solar assistはChargeを止めて一時Standbyへ。
                if action == "thre_day" and value is not None:
                    try:
                        if is_solar_assist_charge_window(datetime.now(JST)) and state["soc"] >= int(value):
                            requests.post(post_control_url("/api/command"), json={"action": "standby"}, timeout=2)
                            g_ctrl_status["status"] = "standby"
                            add_log("手動Day Target", "現在SOCが新Target以上のため、蓄電池をStandbyへ移行しました", "success")
                    except Exception as e:
                        add_log("手動Day Target反映失敗", str(e), "warning")

                return jsonify({"status": "success"})
            else:
                return jsonify({"status": "error", "message": res.get("message", "エラー")})
        except Exception as e:
            return jsonify({"status": "error", "message": f"CTRLサーバーに接続できません: {e}"})

    return jsonify({"status": "success"})

# ==========================================
# 🖥️ フロントエンド UI
# ==========================================
@app.route('/')
def index():
    return r"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Smart Energy Dashboard</title>
        <meta name="apple-mobile-web-app-capable" content="yes">
        <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
        <style>
            :root {
                --bg: #000000; --card-bg: #151517; --border: #2c2c2e; --text-main: #ffffff; --text-sub: #8e8e93;
                --solar: #ffd60a; --grid: #ffffff; --house: #ff453a; --battery: #0a84ff; --tesla: #32d74b; --pure: #bf5af2;
            }
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { 
                background: var(--bg); color: var(--text-main); 
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                height: 100dvh; overflow: hidden; transition: background-color 0.3s;
            }
            #swipe-container {
                display: flex; width: 100vw; height: 100%; overflow-x: auto; overflow-y: hidden;
                scroll-snap-type: x mandatory; scroll-behavior: smooth; -webkit-overflow-scrolling: touch;
            }
            #swipe-container::-webkit-scrollbar { display: none; }
            .page { flex: 0 0 100vw; width: 100vw; height: 100%; scroll-snap-align: start; display: flex; flex-direction: column; gap: 12px; padding: 12px; overflow-y: auto; }
            #indicator-container { position: absolute; bottom: 8px; left: 0; width: 100%; display: flex; justify-content: center; gap: 8px; pointer-events: none; z-index: 100; }
            .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--border); transition: 0.3s; }
            .dot.active { background: var(--text-main); }
            .grafana-container { flex: 1; width: 100%; border-radius: 20px; overflow: hidden; border: 1px solid var(--border); background: var(--card-bg); }
            iframe { width: 100%; height: 100%; border: none; }
            #top-bar { display: flex; justify-content: space-between; align-items: center; padding: 0 10px; flex-shrink: 0; }
            #rate-label { font-size: 1rem; font-weight: 600; color: var(--text-sub); }
            .cheap-active { color: var(--tesla) !important; }
            #rate-timer { color: var(--solar); font-variant-numeric: tabular-nums; font-weight: 600; }
            #clock-area { text-align: center; margin-bottom: 5px; flex-shrink: 0; }
            #current-time { font-size: clamp(3rem, 10vw, 4rem); font-weight: 200; line-height: 1; letter-spacing: -2px; }
            #current-date { color: var(--text-sub); font-size: 0.9rem; font-weight: 500; letter-spacing: 1px; text-transform: uppercase; margin-top: 5px; }
            #dashboard { flex: 1; display: grid; grid-template-columns: repeat(2, 1fr); grid-template-rows: repeat(3, 1fr); gap: 12px; padding-bottom: 10px; min-height: 0; }
            .card { background: var(--card-bg); border-radius: 20px; border: 1px solid var(--border); display: flex; flex-direction: column; justify-content: space-between; position: relative; overflow: hidden; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.4); }
            .interactive-card { cursor: pointer; transition: transform 0.1s; } .interactive-card:active { transform: scale(0.98); }
            .label { color: var(--text-sub); font-size: clamp(0.65rem, 2vw, 0.75rem); font-weight: 600; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; white-space: nowrap; }
            .val-row { display: flex; align-items: baseline; gap: 4px; }
            .val { font-size: clamp(1.8rem, 6vw, 2.5rem); font-weight: 700; line-height: 1; letter-spacing: -1px; white-space: nowrap; }
            .unit { font-size: clamp(0.8rem, 3vw, 1rem); font-weight: 500; color: var(--text-sub); }
            .info-row { display: flex; justify-content: space-between; align-items: flex-end; margin-top: auto; padding-top: 10px; }
            .sub { font-size: clamp(0.7rem, 2.5vw, 0.85rem); color: var(--text-sub); font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
            .highlight-sub { font-weight: 700; color: var(--text-main); }
            .pure-val { color: var(--pure); }
            .progress-bg { width: 100%; height: 6px; background: rgba(255,255,255,0.1); border-radius: 10px; margin-top: 10px; overflow: hidden; flex-shrink: 0; display: flex; }
            .progress-fill { height: 100%; border-radius: 10px; transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1); }
            .mix-fill { border-radius: 0 !important; } 
            .charging-flow { background-image: linear-gradient(90deg, rgba(255,255,255,0.2) 25%, transparent 25%, transparent 50%, rgba(255,255,255,0.2) 50%, rgba(255,255,255,0.2) 75%, transparent 75%, transparent); background-size: 20px 20px; animation: flow-right var(--flow-duration, 1s) linear infinite; }
            .mini-refresh-btn { border: 1px solid var(--border); background: rgba(255,255,255,0.06); color: var(--text-main); border-radius: 999px; padding: 3px 8px; font-size: 0.68rem; font-weight: 800; margin-left: 6px; cursor: pointer; }
            .mini-refresh-btn:active { transform: scale(0.94); }
            .mini-refresh-btn:disabled { opacity: 0.45; cursor: default; }
            @keyframes flow-right { from { background-position: 0 0; } to { background-position: 20px 0; } }
            .tesla-power { font-size: clamp(1rem, 3vw, 1.2rem); font-weight: 700; color: var(--tesla); text-align: right; white-space: nowrap; }
            .t-info-grid { display: flex; justify-content: space-between; align-items: center; margin-top: auto; padding-top: 10px; width: 100%; }
            .source-legend { display: flex; gap: clamp(4px, 2vw, 8px); flex-wrap: wrap; align-items: center; }
            .source-note { font-size: clamp(0.62rem, 2vw, 0.72rem); color: var(--text-sub); margin-top: 4px; opacity: .8; }
            .t-info-left { text-align: left; } .t-info-right { text-align: right; }
            .predict-row { display: flex; justify-content: space-between; align-items: center; gap: 8px; width: 100%; height: 100%; margin-top: 8px; }
            .p-item { display: flex; flex-direction: column; justify-content: center; align-items: center; background: rgba(255,255,255,0.04); padding: 5px; border-radius: 12px; flex: 1; height: 100%; }
            .p-time { font-size: clamp(0.6rem, 2vw, 0.65rem); color: var(--text-sub); font-weight: 600; margin-bottom: 2px; }
            .p-soc { font-size: clamp(1rem, 3.5vw, 1.3rem); font-weight: 700; color: var(--text-main); line-height: 1.1; }
            .p-wh { font-size: clamp(0.55rem, 1.8vw, 0.65rem); color: var(--text-sub); margin-top: 2px; }
            .overlay { position: absolute; top: 0; left: 0; width: 100%; height: 100%; background: rgba(21, 21, 23, 0.95); backdrop-filter: blur(12px); display: flex; flex-direction: column; justify-content: center; align-items: center; opacity: 0; pointer-events: none; transition: opacity 0.3s ease; z-index: 10; border-radius: 20px; padding: 15px; }
            .calc-row { display: flex; justify-content: space-between; width: 100%; font-size: clamp(0.7rem, 2.5vw, 0.8rem); color: var(--text-sub); margin: 3px 0; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 3px; }
            .calc-val { color: var(--text-main); font-family: monospace; font-weight: bold; }
            .cmd-btn { flex: 1; padding: 18px 10px; border-radius: 15px; background: rgba(255,255,255,0.05); color: white; border: 1px solid var(--border); font-weight: bold; cursor: pointer; transition: all 0.2s; font-size: 1rem; }
            .cmd-btn:active { transform: scale(0.95); }
            .cmd-btn.primary { background: rgba(10, 132, 255, 0.2); color: var(--battery); border-color: var(--battery); }
            .tab-container { display: flex; border-bottom: 1px solid var(--border); margin-top: 10px; margin-bottom: 10px; }
            .tab-btn { flex: 1; text-align: center; padding: 10px; color: var(--text-sub); cursor: pointer; font-weight: 600; font-size: 0.9rem; transition: 0.2s; border-bottom: 2px solid transparent; }
            .tab-btn.active { color: var(--solar); border-bottom: 2px solid var(--solar); }
            .tab-content { display: none; max-height: 180px; overflow-y: auto; padding-right: 5px; }
            .tab-content::-webkit-scrollbar { width: 4px; } .tab-content::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 4px; }
            .toggle-switch { position: relative; display: inline-block; width: 44px; height: 24px; }
            .toggle-switch input { opacity: 0; width: 0; height: 0; }
            .toggle-slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: rgba(255,255,255,0.2); transition: .4s; border-radius: 24px; }
            .toggle-slider:before { position: absolute; content: ""; height: 18px; width: 18px; left: 3px; bottom: 3px; background-color: white; transition: .4s; border-radius: 50%; }
            input:checked + .toggle-slider { background-color: var(--solar); }
            input:checked + .toggle-slider:before { transform: translateX(20px); }
            .toggle-tesla input:checked + .toggle-slider { background-color: var(--tesla); }
            .toggle-panic input:checked + .toggle-slider { background-color: var(--house); }
            .toggle-label { font-size: 0.8rem; color: var(--text-main); font-weight: bold; margin-left: 8px; }
            #log-list-container { flex: 1; overflow-y: auto; padding-right: 5px; margin-top: 10px; display: flex; flex-direction: column; gap: 8px; }
            #log-list-container::-webkit-scrollbar { width: 4px; } #log-list-container::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 4px; }
            .log-item { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 12px 15px; display: flex; gap: 12px; align-items: center; border-left: 4px solid var(--text-sub); flex-shrink: 0; }
            .level-critical { border-left-color: var(--house); } .level-warning { border-left-color: var(--solar); } .level-success { border-left-color: var(--tesla); } .level-info { border-left-color: var(--battery); }
            .log-time { font-size: 0.75rem; color: var(--text-sub); font-family: monospace; white-space: nowrap; }
            .log-content { flex: 1; } .log-event { font-size: 0.75rem; color: var(--text-sub); font-weight: bold; margin-bottom: 2px; } .log-action { font-size: 0.9rem; color: var(--text-main); font-weight: 500; }
            .empty-log { color: var(--text-sub); text-align: center; margin-top: 20px; font-size: 0.9rem; }

            .solar-warning { background: rgba(255, 214, 10, 0.12); border: 1px solid rgba(255, 214, 10, 0.35); border-left: 4px solid var(--solar); border-radius: 16px; padding: 12px; margin-top: 10px; display: none; }
            .solar-warning.success { background: rgba(50, 215, 75, 0.10); border-color: rgba(50, 215, 75, 0.32); border-left-color: var(--tesla); }
            .solar-warning-title { font-weight: 800; font-size: 0.9rem; margin-bottom: 5px; }
            .solar-warning-msg { color: var(--text-sub); font-size: 0.78rem; line-height: 1.4; }
            .solar-warning ul { margin: 8px 0 0 18px; color: var(--text-sub); font-size: 0.75rem; line-height: 1.45; }
            .flow-event { background: var(--card-bg); border: 1px solid var(--border); border-radius: 14px; padding: 12px; margin-bottom: 8px; display: grid; grid-template-columns: 52px 1fr; gap: 10px; align-items: start; border-left: 4px solid var(--battery); }
            .flow-event.success { border-left-color: var(--tesla); }
            .flow-event.warning { border-left-color: var(--solar); }
            .flow-event.critical { border-left-color: var(--house); }
            .flow-time { font-family: monospace; color: var(--text-sub); font-size: 0.72rem; font-weight: 700; }
            .flow-title { font-weight: 800; font-size: 0.9rem; margin-bottom: 3px; }
            .flow-detail { color: var(--text-sub); font-size: 0.76rem; line-height: 1.4; }
            .flow-action { margin-top: 6px; display: inline-block; color: var(--text-main); background: rgba(255,255,255,0.06); border: 1px solid var(--border); border-radius: 999px; padding: 3px 8px; font-size: 0.68rem; font-weight: 700; }

            .help-btn { display: inline-flex; align-items: center; justify-content: center; width: 20px; height: 20px; border-radius: 50%; border: 1px solid var(--border); color: var(--text-sub); background: rgba(255,255,255,0.06); font-size: 0.75rem; font-weight: 900; margin-left: 6px; cursor: pointer; }
            .help-btn:active { transform: scale(0.94); }
            .help-modal { position: fixed; inset: 0; background: rgba(0,0,0,0.72); backdrop-filter: blur(8px); display: none; align-items: center; justify-content: center; padding: 22px; z-index: 9999; }
            .help-modal.show { display: flex; }
            .help-card { width: min(520px, 100%); max-height: 80dvh; overflow-y: auto; background: #1c1c1e; border: 1px solid var(--border); border-radius: 22px; padding: 18px; box-shadow: 0 20px 80px rgba(0,0,0,0.55); }
            .help-title { font-size: 1.2rem; font-weight: 850; margin-bottom: 8px; }
            .help-body { color: var(--text-sub); font-size: 0.9rem; line-height: 1.55; white-space: pre-wrap; }
            .help-close { margin-top: 16px; width: 100%; padding: 12px; border-radius: 14px; border: 1px solid var(--border); background: rgba(255,255,255,0.08); color: var(--text-main); font-weight: 800; }
            .debug-row { background: rgba(255,255,255,0.04); border: 1px solid var(--border); border-radius: 12px; padding: 10px; margin-bottom: 8px; font-size: 0.75rem; }
            .debug-meta { color: var(--text-sub); font-family: monospace; margin-bottom: 4px; }
            .debug-event { font-weight: 800; margin-bottom: 4px; }
            .debug-detail { color: var(--text-sub); line-height: 1.4; }
            .debug-json { color: #9cdcfe; background: #080808; border-radius: 8px; padding: 8px; overflow-x: auto; margin-top: 6px; white-space: pre; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.68rem; }

            .copy-settings-card { background: rgba(255,255,255,0.04); border: 1px solid var(--border); border-radius: 14px; padding: 12px; margin-bottom: 10px; }
            .copy-settings-title { font-weight: 800; font-size: 0.9rem; margin-bottom: 4px; display:flex; align-items:center; gap:6px; }
            .copy-settings-desc { color: var(--text-sub); font-size: 0.76rem; line-height: 1.4; margin-bottom: 10px; }
            .copy-btn { width: 100%; padding: 12px; border-radius: 14px; border: 1px solid var(--border); background: rgba(255,255,255,0.08); color: var(--text-main); font-weight: 850; }
            .copy-btn:active { transform: scale(0.98); }
            .copy-toast { position: fixed; left: 50%; bottom: 22px; transform: translateX(-50%) translateY(20px); opacity: 0; pointer-events: none; background: rgba(20,20,22,0.95); border: 1px solid var(--border); color: var(--text-main); padding: 10px 14px; border-radius: 999px; font-weight: 800; font-size: 0.82rem; z-index: 10000; transition: all .22s ease; box-shadow: 0 10px 35px rgba(0,0,0,.35); }
            .copy-toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }


            .number-stepper-box { margin-top: 8px; }
            .number-stepper { display: grid; grid-template-columns: 44px 1fr 44px; gap: 8px; align-items: center; margin: 6px 0; }
            .number-stepper button { height: 42px; border-radius: 12px; border: 1px solid var(--border); background: rgba(255,255,255,0.08); color: var(--text-main); font-size: 1.15rem; font-weight: 900; }
            .number-stepper input { width: 100%; height: 42px; text-align: center; border-radius: 12px; border: 1px solid var(--border); background: rgba(0,0,0,0.22); color: var(--text-main); font-size: 1rem; font-weight: 800; }
            .number-preset-row { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }
            .number-preset-btn { padding: 7px 10px; border-radius: 999px; border: 1px solid var(--border); background: rgba(255,255,255,0.06); color: var(--text-sub); font-size: 0.72rem; font-weight: 800; }
            .number-preset-btn:active, .number-stepper button:active { transform: scale(0.96); }
            .range-enhanced { opacity: 0.35; }



            .cfg-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
            .cfg-label { font-size: 0.85rem; color: var(--text-sub); font-weight: bold; }
            .cfg-input { background: rgba(255,255,255,0.05); border: 1px solid var(--border); color: var(--text-main); border-radius: 8px; padding: 8px 12px; width: 120px; text-align: right; font-family: monospace; font-size: 1rem; }
            @media (max-width: 600px) {
                #dashboard { grid-template-rows: repeat(3, minmax(0, 1fr)); }
                .val { font-size: clamp(1.5rem, 8vw, 2rem); }
                .predict-row { flex-direction: column; gap: 4px; justify-content: center; }
                .p-item { width: 100%; flex-direction: row; justify-content: space-between; padding: 4px 10px; }
                .p-time { margin-bottom: 0; width: 40px; text-align: left; }
                .p-soc { font-size: 1.1rem; width: 40px; text-align: center; }
                .p-wh { margin-top: 0; width: 50px; text-align: right; }
                .t-info-grid { flex-direction: column; align-items: flex-start; gap: 4px; padding-top: 8px; }
                .t-info-right { text-align: left; }
            }
        
.copy-output-modal { position: fixed; inset: 0; background: rgba(0,0,0,0.72); backdrop-filter: blur(8px); display: none; align-items: center; justify-content: center; padding: 18px; z-index: 10001; }
.copy-output-modal.show { display: flex; }
.copy-output-card { width: min(720px, 100%); max-height: 86dvh; background: #1c1c1e; border: 1px solid var(--border); border-radius: 22px; padding: 16px; box-shadow: 0 20px 80px rgba(0,0,0,0.55); display: flex; flex-direction: column; gap: 10px; }
.copy-output-title { font-size: 1.05rem; font-weight: 850; }
.copy-output-desc { color: var(--text-sub); font-size: 0.8rem; line-height: 1.45; }
.copy-output-textarea { width: 100%; min-height: 46dvh; resize: vertical; border-radius: 14px; border: 1px solid var(--border); background: #070707; color: #d7eaff; padding: 12px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.72rem; line-height: 1.45; white-space: pre; }
.copy-output-actions { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.copy-output-actions button { padding: 12px; border-radius: 14px; border: 1px solid var(--border); background: rgba(255,255,255,0.08); color: var(--text-main); font-weight: 850; }
.copy-output-actions .primary { background: rgba(10,132,255,0.22); border-color: rgba(10,132,255,0.45); }


            .proposal-card { border: 1px solid var(--border); background: var(--card-bg); border-radius: 18px; padding: 14px; margin-bottom: 10px; }
            .proposal-card.warning { border-color: rgba(255,214,10,.45); background: rgba(255,214,10,.08); }
            .proposal-card.critical { border-color: rgba(255,69,58,.55); background: rgba(255,69,58,.10); }
            .proposal-card.success { border-color: rgba(50,215,75,.45); background: rgba(50,215,75,.08); }
            .proposal-title { font-size: 1.05rem; font-weight: 850; margin-bottom: 6px; color: var(--text-main); }
            .proposal-message { color: var(--text-sub); font-size: .84rem; line-height: 1.45; margin-bottom: 10px; }
            .proposal-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; margin-top: 10px; }
            .proposal-metric { background: rgba(0,0,0,.18); border: 1px solid var(--border); border-radius: 13px; padding: 10px; }
            .proposal-metric-label { color: var(--text-sub); font-size: .68rem; font-weight: 800; letter-spacing: .04em; text-transform: uppercase; }
            .proposal-metric-value { font-size: 1.25rem; font-weight: 850; margin-top: 4px; color: var(--text-main); }
            .proposal-reasons { margin: 10px 0 0 18px; color: var(--text-sub); font-size: .78rem; line-height: 1.5; padding: 0; }
            .proposal-actions { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 12px; }
            .proposal-actions button { border: 1px solid var(--border); border-radius: 13px; padding: 12px 10px; color: var(--text-main); background: rgba(255,255,255,.07); font-weight: 850; }
            .proposal-actions .apply { background: rgba(50,215,75,.18); border-color: rgba(50,215,75,.42); }
            .proposal-actions .ignore { background: rgba(255,255,255,.05); color: var(--text-sub); }


.metric-grid { display:grid; }
.metric-card { background:rgba(255,255,255,.045); border:1px solid var(--border); border-radius:12px; padding:10px; }
.metric-label { color:var(--text-sub); font-size:.7rem; font-weight:800; margin-bottom:4px; }
.metric-value { font-weight:900; font-size:.96rem; }
.ctrl-btn:disabled { opacity:.45; pointer-events:none; }


.flow-summary-card { background: rgba(255,255,255,.045); border: 1px solid var(--border); border-radius: 16px; padding: 14px; margin-bottom: 12px; border-left: 4px solid var(--tesla); }
.flow-summary-card.day { border-left-color: var(--solar); }
.flow-summary-card.night { border-left-color: #5e5ce6; }
.flow-summary-card.error { border-left-color: var(--house); }
.flow-summary-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; }
.flow-phase-badge { font-size: .7rem; font-weight: 900; color: var(--text-main); border: 1px solid var(--border); border-radius: 999px; padding: 4px 8px; white-space: nowrap; background: rgba(255,255,255,.06); }
.flow-policy { font-size: 1.02rem; font-weight: 900; margin-bottom: 4px; }
.flow-reason { color: var(--text-sub); font-size: .78rem; line-height: 1.45; }
.flow-next { margin-top: 10px; background: rgba(10,132,255,.12); border: 1px solid rgba(10,132,255,.25); border-radius: 12px; padding: 10px; font-size: .78rem; line-height: 1.45; }
.flow-next-title { font-weight: 900; margin-bottom: 3px; }
.flow-kv-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 7px; margin-top: 10px; }
.flow-kv { background: rgba(0,0,0,.16); border: 1px solid var(--border); border-radius: 10px; padding: 8px; }
.flow-kv-label { color: var(--text-sub); font-size: .66rem; font-weight: 800; margin-bottom: 3px; }
.flow-kv-value { font-size: .82rem; font-weight: 900; word-break: break-word; }
.flow-alert { margin-top: 8px; background: rgba(255,214,10,.12); border: 1px solid rgba(255,214,10,.3); border-radius: 10px; padding: 8px; color: var(--text-main); font-size: .74rem; line-height: 1.4; }
.flow-section-title { color: var(--text-sub); font-size: .72rem; font-weight: 900; margin: 12px 0 6px; letter-spacing: .02em; }

</style>
    </head>
    <body>
        <div id="swipe-container">
            <div class="page" id="page-settings">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;">
                    <div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">System Config</div>
                    <div style="color: var(--text-sub); font-size: 0.85rem; margin-top: 5px;">Swipe left for Logs</div>
                </div>
                <div class="card" style="flex: 1; overflow-y: auto; justify-content: flex-start;">

                <div class="copy-settings-card">
                    <div class="copy-settings-title">設定値エクスポート <span class="help-btn" onclick="showHelp('copy_settings')">?</span></div>
                    <div class="copy-settings-desc">現在の制御設定・システム設定・Afternoon判定・Tesla状態をまとめてクリップボードへコピーします。</div>
                    <button class="copy-btn" onclick="copyCurrentSettings()">現在の設定値をコピー</button>
                </div>

                    <div class="label" style="margin-bottom: 15px;">Hardware Limits & Thresholds <span class="help-btn" onclick="showHelp('hardware_limits')">?</span></div>
                    
                    <div class="cfg-row"><span class="cfg-label">太陽光パネル容量 (W)</span><input type="number" class="cfg-input" id="cfg_SYSTEM_CAPACITY_W"></div>
                    <div class="cfg-row"><span class="cfg-label">システム損失係数</span><input type="number" step="0.01" class="cfg-input" id="cfg_SYSTEM_LOSS_FACTOR"></div>
                    <div class="cfg-row"><span class="cfg-label">蓄電池最大容量 (Wh)</span><input type="number" class="cfg-input" id="cfg_BATT_MAX_WH"></div>
                    <div class="cfg-row"><span class="cfg-label">契約グリッド上限 (W)</span><input type="number" class="cfg-input" id="cfg_GRID_LIMIT_W"></div>
                    <div class="cfg-row"><span class="cfg-label">警告ブレーカー限界 (W)</span><input type="number" class="cfg-input" id="cfg_WARNING_LIMIT_W" style="color: var(--solar)"></div>
                    <div class="cfg-row"><span class="cfg-label">緊急ブレーカー遮断 (W)</span><input type="number" class="cfg-input" id="cfg_CRITICAL_LIMIT_W" style="color: var(--house)"></div>
                    <div class="cfg-row"><span class="cfg-label">降下ターゲット安全圏 (W)</span><input type="number" class="cfg-input" id="cfg_TARGET_SAFE_POWER_W"></div>
                    <div class="cfg-row"><span class="cfg-label">テスラ最大許容電流 (A)</span><input type="number" class="cfg-input" id="cfg_MAX_CHARGE_AMPS"></div>
                    <div class="cfg-row"><span class="cfg-label">テスラ急速充電発動 (不足km)</span><input type="number" class="cfg-input" id="cfg_TESLA_HEAVY_CHARGE_KM" style="color: var(--tesla)"></div>
                    
                    <div style="margin-top: 15px; border-top: 1px solid var(--border); padding-top: 15px;">
                        <span class="cfg-label">Discord Webhook URL</span>
                        <input type="text" class="cfg-input" id="cfg_DISCORD_WEBHOOK_URL" style="width: 100%; margin-top: 5px; font-size: 0.8rem; text-align: left;">
                    </div>
                    
                    <div style="display: flex; gap: 10px; margin-top: 25px;">
                        <button class="cmd-btn" style="padding: 12px; font-size: 0.9rem;" onclick="resetSysSettings()">🔄 デフォルトに戻す</button>
                        <button class="cmd-btn primary" style="padding: 12px; font-size: 0.9rem;" onclick="saveSysSettings()">💾 保存</button>
                    </div>
                </div>
            </div>

            <div class="page" id="page-log">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;">
                    <div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">System Logs</div>
                    <button class="cmd-btn" style="margin-top: 10px; padding: 6px 15px; font-size: 0.8rem; border-radius: 8px; max-width: 180px; height: auto;" onclick="toggleFileLog()">詳細ファイルログ表示</button>
                </div>
                <div id="file-log-window" style="display: none; background: #000000; border: 1px solid var(--border); border-radius: 12px; padding: 10px; margin-bottom: 5px; max-height: 120px; overflow-y: auto; font-family: monospace; font-size: 0.75rem; color: var(--tesla); text-align: left; white-space: pre-wrap; line-height: 1.4;"></div>
                
                <div class="card" style="flex: 0 0 auto; justify-content: flex-start; padding: 12px; margin-bottom: 5px;">
                    <div style="display:flex; justify-content:space-between; align-items:center; gap:10px;">
                        <div>
                            <div class="label">Debug Logs <span class="help-btn" onclick="showHelp('debug_logs')">?</span></div>
                            <div class="sub">制御判断の入力値と分岐を確認します</div>
                        </div>
                        <label class="toggle-switch toggle-tesla">
                            <input type="checkbox" id="debug-toggle" onchange="setDebugMode(this.checked)">
                            <span class="toggle-slider"></span>
                        </label>
                    </div>
                    <div id="debug-log-window" style="display:none; max-height:180px; overflow-y:auto; margin-top:10px;"></div>
                </div>

                <div id="log-list-container"><div class="empty-log">読み込み中...</div></div>
            </div>

            <div class="page" id="page-flow">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;">
                    <div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">Decision Flow</div>
                    <div style="color: var(--text-sub); font-size: 0.85rem; margin-top: 5px;">AI 24h Rolling Prediction <span class="help-btn" onclick="showHelp('decision_flow')">?</span></div>
                </div>
                <div id="solar-priority-warning" class="solar-warning"><div class="solar-warning-title" id="solar-warning-title"></div><div class="solar-warning-msg" id="solar-warning-msg"></div><ul id="solar-warning-reasons"></ul></div>
                <div id="flow-summary-container" style="overflow-y:auto; max-height:48%; padding:5px;"></div>
                <div id="dynamic-timeline" style="overflow-y: auto; flex: 1; padding: 5px;"></div>
            </div>

            <div class="page" id="page-read">
                <div id="top-bar">
                    <div id="rate-label">---</div>
                    <div style="font-size: 0.8rem; color: var(--text-sub); text-transform: uppercase; font-weight: bold; letter-spacing: 1px;">Dashboard</div>
                    <div id="rate-timer">--:--</div>
                </div>
                <div id="clock-area">
                    <div id="current-time">00:00</div>
                    <div id="current-date">----.--.--</div>
                </div>
                <div id="dashboard">
                    <div class="card">
                        <div class="label" style="color: var(--solar)">Solar Power</div>
                        <div class="val-row"><div class="val" id="solar">0.0</div><div class="unit">kW</div></div>
                        <div class="progress-bg"><div id="solar-bar" class="progress-fill" style="background-color: var(--solar); width: 0%;"></div></div>
                        <div class="info-row"><div class="sub">Panel Cap</div><div class="sub highlight-sub"><span id="dash_sys_cap">2.4</span> kW</div></div>
                    </div>
                    <div class="card">
                        <div class="label" style="color: var(--grid)">Grid Margin</div>
                        <div class="val-row"><div class="val" id="grid">0.0</div><div class="unit">kW</div></div>
                        <div class="progress-bg"><div id="grid-bar" class="progress-fill" style="background-color: var(--grid); width: 0%;"></div></div>
                        <div class="info-row"><div class="sub">Capacity</div><div class="sub highlight-sub"><span id="grid_cap">0.0</span> kW</div></div>
                    </div>
                    <div class="card">
                        <div class="label" style="color: var(--house)">Total Consumption</div>
                        <div class="val-row"><div class="val" id="cons">0.0</div><div class="unit">kW</div></div>
                        <div class="progress-bg" title="Estimated supply mix">
                            <div id="cons-solar-bar" class="progress-fill mix-fill" style="background-color: var(--solar); width: 0%;"></div>
                            <div id="cons-battery-bar" class="progress-fill mix-fill" style="background-color: var(--battery); width: 0%;"></div>
                            <div id="cons-grid-bar" class="progress-fill mix-fill" style="background-color: var(--grid); width: 0%;"></div>
                        </div>
                        <div class="info-row">
                            <div class="sub">Supply Mix</div>
                            <div class="sub highlight-sub" id="total-source-mix">S 0.00 / B 0.00 / G 0.00 kW</div>
                        </div>
                    </div>
                    <div class="card interactive-card" onclick="showBatteryPrediction()">
                        <div class="label" style="color: var(--battery)">House Battery (Tap)</div>
                        <div class="val-row"><div class="val" id="soc_main">0</div><div class="unit">%</div></div>
                        <div class="progress-bg"><div id="battery-bar" class="progress-fill" style="background-color: var(--battery); width: 0%;"></div></div>
                        <div class="info-row"><div class="sub">Flow</div><div class="sub highlight-sub" id="b-flow">0 W</div></div>
                        <div id="predict-overlay" class="overlay">
                            <div class="label" style="color: var(--solar); margin-bottom: 6px;">Arrival Forecast</div>
                            <div style="width: 100%; margin-bottom: 6px; background: rgba(0,0,0,0.4); padding: 8px; border-radius: 12px;">
                                <div class="calc-row"><span>P1 (Current)</span><span class="calc-val" id="calc-p1">--</span></div>
                                <div class="calc-row"><span>Grid Avail.</span><span class="calc-val" id="calc-avail" style="color: var(--pure);">--</span></div>
                                <div class="calc-row" style="border-bottom: none; padding-bottom: 0;"><span>P2 (Shifted)</span><span class="calc-val" id="calc-p2">--</span></div>
                            </div>
                            <div class="val-row"><div class="val" id="po-soc" style="color: var(--text-main);">--</div><div class="unit">%</div></div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="label" style="color: var(--tesla)">Tesla</div>
                        <div style="display: flex; justify-content: space-between; align-items: baseline; gap: 8px;">
                            <div class="val-row"><div class="val" id="t-soc-main">--</div><div class="unit">%</div></div>
                            <div id="t-power" class="tesla-power">0.00 kW</div>
                        </div>
                        <div class="progress-bg"><div id="tesla-soc-bar" class="progress-fill" style="background-color: var(--tesla); width: 0%;"></div></div>
                        <div class="t-info-grid">
                            <div class="t-info-left"><div class="sub">Range</div><div class="sub highlight-sub" id="t-range-km" style="color: var(--text-main)">-- km</div></div>
                            <div class="t-info-right"><div class="sub">Need</div><div class="sub highlight-sub" id="t-missing-km" style="color: var(--text-main)">-- km</div></div>
                        </div>
                        <div class="source-note">Wall <span id="t-wall-kw">0.00</span> kW / <span id="t-load-type">none</span><button id="tesla-refresh-btn" class="mini-refresh-btn" onclick="manualFleetRefresh(event)">更新</button></div>
                    </div>
                    <div class="card"><div class="label">Battery Forecast</div><div class="predict-row" id="predict-area"></div></div>
                </div>
            </div>

            <div class="page" id="page-write">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;">
                    <div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">Control Center</div>
                </div>

                <div class="card" style="flex-shrink: 0; padding: 15px; border-color: var(--house);">
                    <div class="label" style="color: var(--house);">Master Lock Option</div>
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 5px;">
                        <div>
                            <div style="color: var(--text-main); font-size: 0.9rem; font-weight: bold;">🔒 読み取り専用モード</div>
                            <div style="color: var(--text-sub); font-size: 0.75rem; margin-top: 2px;">ONにするとAI自動制御が完全に停止します。</div>
                        </div>
                        <label class="toggle-switch toggle-panic">
                            <input type="checkbox" id="read-only-toggle" onchange="toggleReadOnly(this.checked)"><span class="toggle-slider"></span>
                        </label>
                    </div>
                </div>

                <div class="card" style="flex-shrink: 0; margin-top: 5px; max-height: 220px;">
                    <div class="label" style="color: var(--solar); font-size: 0.85rem;">Solar Forecast</div>
                    <div class="tab-container">
                        <div class="tab-btn active" id="tab-today" onclick="switchTab('today')">Today (--)</div>
                        <div class="tab-btn" id="tab-tomorrow" onclick="switchTab('tomorrow')">Tomorrow (--)</div>
                    </div>
                    <div id="forecast-content-today" class="tab-content" style="display: block;"></div>
                    <div id="forecast-content-tomorrow" class="tab-content" style="display: none;"></div>
                </div>

                <div class="card" style="flex-shrink: 0; margin-top: 10px;">
                    <div class="label" style="color: var(--battery)">House Battery Automation</div>
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 10px;">
                        <div style="color: var(--text-main); font-size: 0.85rem; font-weight: bold;">🌙 Night Target (primary charge window)</div>
                    </div>
                    <div style="display: flex; align-items: center; gap: 10px; margin-top: 5px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <input type="range" id="night-slider" min="0" max="100" step="5" value="30" style="flex: 1; accent-color: var(--battery);" oninput="document.getElementById('night-val').innerText = this.value + '%'">
                        <span id="night-val" style="font-weight: bold; width: 45px; text-align: right; font-size: 1.1rem; color: var(--text-main);">30%</span>
                        <button class="cmd-btn" style="flex: 0; padding: 10px 15px; border-radius: 10px; font-size: 0.9rem;" onclick="sendThreshold('thre_night', document.getElementById('night-slider').value)">Set</button>
                    </div>

                    <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 15px;">
                        <div style="color: var(--text-main); font-size: 0.85rem; font-weight: bold;">☀️ Day Target (solar assist window)</div>
                        <div style="display: flex; align-items: center;">
                            <label class="toggle-switch"><input type="checkbox" id="day-auto-toggle" onchange="toggleDayAuto(this.checked)" checked><span class="toggle-slider"></span></label>
                            <span class="toggle-label" style="color: var(--solar);">AI Auto</span>
                        </div>
                    </div>
                    <div id="day-slider-container" style="display: flex; align-items: center; gap: 10px; margin-top: 5px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <input type="range" id="day-slider" min="0" max="100" step="1" value="30" style="flex: 1; accent-color: var(--solar);" oninput="document.getElementById('day-val').innerText = this.value + '%'">
                        <span id="day-val" style="font-weight: bold; width: 45px; text-align: right; font-size: 1.1rem; color: var(--text-main);">30%</span>
                        <button id="day-set-btn" class="cmd-btn" style="flex: 0; padding: 10px 15px; border-radius: 10px; font-size: 0.9rem;" onclick="sendThreshold('thre_day', document.getElementById('day-slider').value)">Set</button>
                    </div>

                    <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 15px;">
                        <div>
                            <div style="color: var(--text-main); font-size: 0.85rem; font-weight: bold;">🌇 Afternoon Target</div>
                            <div style="color: var(--text-sub); font-size: 0.72rem; margin-top: 2px;">夕方までに確保したい蓄電池SOC</div>
                        </div>
                        <div style="display: flex; align-items: center;">
                            <label class="toggle-switch"><input type="checkbox" id="afternoon-recovery-toggle" onchange="toggleAfternoonRecovery(this.checked)" checked><span class="toggle-slider"></span></label>
                            <span class="toggle-label" style="color: var(--battery);">Recovery</span>
                        </div>
                    </div>
                    <div style="display: flex; align-items: center; gap: 10px; margin-top: 5px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <input type="range" id="afternoon-target-slider" min="0" max="100" step="1" value="70" style="flex: 1; accent-color: var(--battery);" oninput="document.getElementById('afternoon-target-val').innerText = this.value + '%'">
                        <span id="afternoon-target-val" style="font-weight: bold; width: 45px; text-align: right; font-size: 1.1rem; color: var(--text-main);">70%</span>
                        <button class="cmd-btn" style="flex: 0; padding: 10px 15px; border-radius: 10px; font-size: 0.9rem;" onclick="setAfternoonTargetSoc(document.getElementById('afternoon-target-slider').value)">Set</button>
                    </div>
                    <div style="display: flex; align-items: center; gap: 10px; margin-top: 5px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <span style="color: var(--text-sub); font-size: 0.8rem; font-weight: bold;">Target Hour</span>
                        <input type="range" id="afternoon-hour-slider" min="14" max="19" step="1" value="17" style="flex: 1; accent-color: var(--battery);" oninput="document.getElementById('afternoon-hour-val').innerText = this.value + ':00'">
                        <span id="afternoon-hour-val" style="font-weight: bold; width: 55px; text-align: right; font-size: 1.0rem; color: var(--text-main);">17:00</span>
                        <button class="cmd-btn" style="flex: 0; padding: 10px 15px; border-radius: 10px; font-size: 0.9rem;" onclick="setAfternoonTargetHour(document.getElementById('afternoon-hour-slider').value)">Set</button>
                    </div>
                    <div id="afternoon-recovery-info" style="font-size: 0.75rem; color: var(--text-sub); margin-top: 8px; line-height: 1.4; background: rgba(255,255,255,0.04); padding: 8px 10px; border-radius: 10px;">
                        solar window後回復判定: --
                    </div>
                </div>

                <div class="card" style="flex-shrink: 0; margin-top: 10px;">
                    <div class="label" style="color: var(--battery)">Manual Override</div>
                    <div style="display: flex; gap: 8px; margin-top: 5px;">
                        <button class="cmd-btn" onclick="sendCommand('battery', 'auto')">🔄 Auto</button>
                        <button class="cmd-btn primary" onclick="sendCommand('battery', 'charge')">⚡️ Charge</button>
                        <button class="cmd-btn" onclick="sendCommand('battery', 'standby')">⏸️ Standby</button>
                    </div>
                </div>

                <div class="card" style="flex-shrink: 0; margin-top: 10px;">
                    <div class="label" style="color: var(--tesla)">Tesla Automation</div>
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 5px;">
                        <div style="color: var(--text-main); font-size: 0.85rem; font-weight: bold;">🚗 Smart Charge (余剰追従)</div>
                        <div style="display: flex; align-items: center;">
                            <label class="toggle-switch toggle-tesla"><input type="checkbox" id="tesla-auto-toggle" onchange="toggleTeslaAuto(this.checked)" checked><span class="toggle-slider"></span></label>
                            <span class="toggle-label" style="color: var(--tesla);">Auto</span>
                        </div>
                    </div>
                    <div style="display: flex; align-items: center; gap: 10px; margin-top: 5px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <input type="range" id="tesla-min-soc-slider" min="0" max="100" step="5" value="50" style="flex: 1; accent-color: var(--battery);" oninput="document.getElementById('tesla-min-soc-val').innerText = this.value + '%'">
                        <span id="tesla-min-soc-val" style="font-weight: bold; width: 45px; text-align: right; font-size: 1.1rem; color: var(--text-main);">50%</span>
                        <button class="cmd-btn" style="flex: 0; padding: 10px 15px; border-radius: 10px; font-size: 0.9rem;" onclick="setTeslaMinSoc(document.getElementById('tesla-min-soc-slider').value)">Set</button>
                    </div>
                    <div style="display: flex; align-items: center; gap: 10px; margin-top: 8px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <span style="color: var(--text-sub); font-size: 0.8rem; font-weight: bold;">Mode</span>
                        <select id="tesla-charge-mode-select" onchange="setTeslaChargeMode(this.value)" style="flex:1; background: rgba(255,255,255,0.08); color: var(--text-main); border: 1px solid var(--border); border-radius: 10px; padding: 10px; font-weight: bold;">
                            <option value="auto">Auto</option>
                            <option value="solar_priority">Solar Priority</option>
                            <option value="manual">Manual</option>
                        </select>
                    </div>
                    <div style="display: flex; align-items: center; gap: 10px; margin-top: 5px; background: rgba(255,255,255,0.05); padding: 10px; border-radius: 12px;">
                        <span style="color: var(--text-sub); font-size: 0.8rem; font-weight: bold;">Grid Assist <span class="help-btn" onclick="showHelp('grid_assist')">?</span></span>
                        <input type="range" id="grid-assist-slider" min="0" max="1000" step="50" value="200" style="flex: 1; accent-color: var(--tesla);" oninput="document.getElementById('grid-assist-val').innerText = this.value + 'W'">
                        <span id="grid-assist-val" style="font-weight: bold; width: 60px; text-align: right; font-size: 1.0rem; color: var(--text-main);">200W</span>
                        <button class="cmd-btn" style="flex: 0; padding: 10px 15px; border-radius: 10px; font-size: 0.9rem;" onclick="setSolarPriorityGridAssist(document.getElementById('grid-assist-slider').value)">Set</button>
                    </div>
                </div>
            </div>


            <div class="page" id="page-proposal">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;">
                    <div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">Proposal</div>
                    <div style="color: var(--text-sub); font-size: .82rem; margin-top: 4px;">Low Solar Day Guard</div>
                </div>

                <div id="proposal-main-card" class="proposal-card">
                    <div class="proposal-title" id="proposal-title">提案を計算中...</div>
                    <div class="proposal-message" id="proposal-message">明日の発電予測とTesla不足量から、Night/Day Targetの推奨値を表示します。</div>
                    <ul class="proposal-reasons" id="proposal-reasons"></ul>
                    <div class="proposal-grid">
                        <div class="proposal-metric"><div class="proposal-metric-label">Tomorrow Solar</div><div class="proposal-metric-value" id="proposal-solar">-- kWh</div></div>
                        <div class="proposal-metric"><div class="proposal-metric-label">Tesla Missing</div><div class="proposal-metric-value" id="proposal-tesla">-- km</div></div>
                        <div class="proposal-metric"><div class="proposal-metric-label">Night Target</div><div class="proposal-metric-value" id="proposal-night">-- → --%</div></div>
                        <div class="proposal-metric"><div class="proposal-metric-label">Day Target</div><div class="proposal-metric-value" id="proposal-day">-- → --%</div></div>
                    </div>
                    <div class="proposal-actions">
                        <button class="apply" onclick="applyLowSolarProposal()">推奨値を適用</button>
                        <button class="ignore" onclick="dismissLowSolarProposal()">今日は無視</button>
                    </div>
                </div>

                <div class="proposal-card">
                    <div class="proposal-title">提案モード</div>
                    <div class="proposal-message">勝手に設定は変更せず、明日が低発電日っぽいときだけ推奨値を表示します。</div>
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-top:8px;">
                        <div style="font-weight:800; color:var(--text-main);">Low Solar Proposal</div>
                        <label class="toggle-switch"><input type="checkbox" id="proposal-mode-toggle" onchange="toggleProposalMode(this.checked)" checked><span class="toggle-slider"></span></label>
                    </div>
                </div>

                <div class="proposal-card">
                    <div class="proposal-title">判定の考え方 <span class="help-btn" onclick="showHelp('proposal_mode')">?</span></div>
                    <div class="proposal-message">
                        Tesla不足が10km以下なら自然減少レベルとして蓄電池優先。明日発電が少ない場合は、前夜のNight Targetを上げてsolar assist windowの負担を軽くします。
                    </div>
                    <div class="proposal-grid">
                        <div class="proposal-metric"><div class="proposal-metric-label">Low Solar</div><div class="proposal-metric-value" id="proposal-low-th">5.0 kWh</div></div>
                        <div class="proposal-metric"><div class="proposal-metric-label">Storm Solar</div><div class="proposal-metric-value" id="proposal-storm-th">3.0 kWh</div></div>
                        <div class="proposal-metric"><div class="proposal-metric-label">Small Tesla Gap</div><div class="proposal-metric-value" id="proposal-small-km">10 km</div></div>
                        <div class="proposal-metric"><div class="proposal-metric-label">Afternoon Deficit</div><div class="proposal-metric-value" id="proposal-deficit">-- kWh</div></div>
                    </div>
                </div>
            </div>

            <div class="page" id="page-analysis">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;"><div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">Energy Analytics</div></div>
                <div class="grafana-container"><iframe src="http://localhost:3000/d/your-energy-dashboard?orgId=1&refresh=5s&kiosk&from=now%2Fd&to=now%2Fd"></iframe></div>
            </div>
            
            <div class="page" id="page-teslamate">
                <div style="text-align: center; margin: 10px 0; flex-shrink: 0;"><div style="font-size: 2rem; font-weight: 200; letter-spacing: -1px;">TeslaMate</div></div>
                <div class="grafana-container"><iframe src="http://localhost:3000/d/teslamate/overview?orgId=1&from=now-24h&to=now&timezone=browser&var-car_id=$__all&var-length_unit=km&var-temp_unit=C&var-preferred_range=rated&var-base_url=http:%2F%2Flocalhost:4000&refresh=30s&kiosk"></iframe></div>
            </div>
        </div>
            
        <div id="indicator-container">
            <div class="dot" id="dot0"></div>
            <div class="dot" id="dot1"></div>
            <div class="dot" id="dot2"></div>
            <div class="dot active" id="dot3"></div>
            <div class="dot" id="dot4"></div>
            <div class="dot" id="dot5"></div>
            <div class="dot" id="dot6"></div>
            <div class="dot" id="dot7"></div>
        </div>

        <script>
            let lastData = null;
            window.lastForecastCalc = null; 

            const container = document.getElementById('swipe-container');
            const dots = [document.getElementById('dot0'), document.getElementById('dot1'), document.getElementById('dot2'), document.getElementById('dot3'), document.getElementById('dot4'), document.getElementById('dot5'), document.getElementById('dot6'), document.getElementById('dot7')];

            container.addEventListener('scroll', () => {
                const pageIndex = Math.round(container.scrollLeft / container.clientWidth);
                dots.forEach((dot, idx) => { dot.classList.toggle('active', idx === pageIndex); });
            });


            function updateLowSolarProposal(d) {
                const p = d.low_solar_proposal || d.proposal || {};
                const card = document.getElementById('proposal-main-card');
                if (!card) return;

                card.classList.remove('warning', 'critical', 'success', 'info');
                if (p.level) card.classList.add(p.level);

                const title = document.getElementById('proposal-title');
                const message = document.getElementById('proposal-message');
                const solar = document.getElementById('proposal-solar');
                const tesla = document.getElementById('proposal-tesla');
                const night = document.getElementById('proposal-night');
                const day = document.getElementById('proposal-day');
                const lowTh = document.getElementById('proposal-low-th');
                const stormTh = document.getElementById('proposal-storm-th');
                const smallKm = document.getElementById('proposal-small-km');
                const deficit = document.getElementById('proposal-deficit');

                if (title) title.innerText = p.title || '提案なし';
                if (message) {
                    message.innerText = p.message || (
                        p.forecast_total_kwh === 0 || p.tomorrow_total_kwh === 0
                            ? '明日の発電予測を取得中です。Open-Meteo復帰後に再計算します。'
                            : '明日の発電予測から提案を計算します。'
                    );
                }

                const forecastKwh = p.forecast_total_kwh ?? p.tomorrow_total_kwh ?? d.forecast?.tomorrow_total ?? 0;
                if (solar) solar.innerText = `${Number(forecastKwh || 0).toFixed(1)} kWh`;
                if (tesla) tesla.innerText = `${Number(p.tesla_missing_km ?? 0).toFixed(0)} km`;
                if (night) night.innerText = `${p.current_night_target ?? '--'} → ${p.recommended_night_target ?? '--'}%`;
                if (day) day.innerText = `${p.current_day_target ?? '--'} → ${p.recommended_day_target ?? '--'}%`;

                if (lowTh) lowTh.innerText = `${Number(d.settings?.low_solar_threshold_kwh ?? p.low_threshold_kwh ?? 5).toFixed(1)} kWh`;
                if (stormTh) stormTh.innerText = `${Number(d.settings?.storm_solar_threshold_kwh ?? p.severe_threshold_kwh ?? 3).toFixed(1)} kWh`;
                if (smallKm) smallKm.innerText = `${d.settings?.proposal_tesla_small_km ?? p.tesla_small_km ?? 10} km`;
                if (deficit) deficit.innerText = `${Number(p.deficit_kwh ?? p.afternoon?.deficit_kwh ?? 0).toFixed(1)} kWh`;

                const toggle = document.getElementById('proposal-mode-toggle');
                if (toggle) toggle.checked = !!(d.settings?.low_solar_proposal_mode ?? p.enabled ?? true);

                const ul = document.getElementById('proposal-reasons');
                if (ul) {
                    ul.innerHTML = '';
                    const reasons = p.reasons || [];
                    if (reasons.length) {
                        reasons.forEach(r => {
                            const li = document.createElement('li');
                            li.textContent = r;
                            ul.appendChild(li);
                        });
                    } else {
                        const li = document.createElement('li');
                        li.textContent = '現在は特別な提案はありません。';
                        ul.appendChild(li);
                    }
                }

                const applyBtn = card.querySelector('.proposal-actions .apply');
                if (applyBtn) {
                    applyBtn.disabled = !p.active;
                    applyBtn.style.opacity = p.active ? '1' : '0.45';
                }
            }

            async function applyLowSolarProposal() {
                if (!lastData || !lastData.low_solar_proposal) return;
                const p = lastData.low_solar_proposal;
                if (lastData.read_only) { alert('読み取り専用モード中は適用できません。'); return; }
                if (!confirm(`推奨値を適用しますか？
Night Target: ${p.current_night_target}% → ${p.recommended_night_target}%
Day Target: ${p.current_day_target}% → ${p.recommended_day_target}%`)) return;
                try {
                    const r = await fetch('/api/command', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ target: 'settings', action: 'apply_low_solar_proposal', value: { night_target: p.recommended_night_target, day_target: p.recommended_day_target } })
                    });
                    const j = await r.json();
                    if (j.status === 'success') { alert('推奨値を適用しました。'); update(); }
                    else { alert('適用に失敗: ' + (j.message || 'unknown')); }
                } catch(e) { alert('適用に失敗しました: ' + e); }
            }

            async function dismissLowSolarProposal() {
                if (!confirm('今日の低発電日提案を無視しますか？')) return;
                try {
                    await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'dismiss_low_solar_proposal' }) });
                    update();
                } catch(e) {}
            }

            async function toggleProposalMode(isChecked) {
                try {
                    await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_low_solar_proposal_mode', value: isChecked }) });
                    update();
                } catch(e) {}
            }

            function switchTab(tab) {
                document.getElementById('tab-today').classList.remove('active');
                document.getElementById('tab-tomorrow').classList.remove('active');
                document.getElementById('forecast-content-today').style.display = 'none';
                document.getElementById('forecast-content-tomorrow').style.display = 'none';
                document.getElementById('tab-' + tab).classList.add('active');
                document.getElementById('forecast-content-' + tab).style.display = 'block';
            }

            function buildHourlyHTML(hourlyData, total, sys_cap) {
                if(!hourlyData || hourlyData.length === 0) return '';
                let html = `<div style="font-size: 0.8rem; color: var(--text-sub); text-align: right; margin-bottom: 8px;">合計予測: <span style="color:var(--text-main); font-weight:bold;">${total}</span> kWh</div>`;
                const kw_cap = (sys_cap || 2400) / 1000;
                hourlyData.forEach(h => {
                    let pct = Math.min(100, (h.gen / kw_cap) * 100); 
                    html += `
                        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 6px; font-size: 0.85rem;">
                            <div style="width: 45px; color: var(--text-sub);">${h.time}</div>
                            <div style="width: 45px; text-align: right; font-weight: bold;">${h.gen.toFixed(2)}</div>
                            <div style="flex: 1; height: 10px; background: rgba(255,255,255,0.1); border-radius: 5px; overflow: hidden;">
                                <div style="width: ${pct}%; height: 100%; background: var(--solar); border-radius: 5px;"></div>
                            </div>
                        </div>
                    `;
                });
                return html;
            }

            async function toggleFileLog() {
                const win = document.getElementById('file-log-window');
                if (win.style.display === 'block') { win.style.display = 'none'; }
                else {
                    try {
                        const r = await fetch('/api/logs/file');
                        const d = await r.json();
                        win.innerText = d.content; win.style.display = 'block'; win.scrollTop = win.scrollHeight;
                    } catch(e) { win.innerText = "同期失敗"; win.style.display = 'block'; }
                }
            }

            async function saveSysSettings() {
                if(!confirm("設定を保存してシステムに適用しますか？")) return;
                const newSet = {
                    "SYSTEM_CAPACITY_W": document.getElementById('cfg_SYSTEM_CAPACITY_W').value,
                    "SYSTEM_LOSS_FACTOR": document.getElementById('cfg_SYSTEM_LOSS_FACTOR').value,
                    "BATT_MAX_WH": document.getElementById('cfg_BATT_MAX_WH').value,
                    "GRID_LIMIT_W": document.getElementById('cfg_GRID_LIMIT_W').value,
                    "WARNING_LIMIT_W": document.getElementById('cfg_WARNING_LIMIT_W').value,
                    "CRITICAL_LIMIT_W": document.getElementById('cfg_CRITICAL_LIMIT_W').value,
                    "TARGET_SAFE_POWER_W": document.getElementById('cfg_TARGET_SAFE_POWER_W').value,
                    "MAX_CHARGE_AMPS": document.getElementById('cfg_MAX_CHARGE_AMPS').value,
                    "TESLA_HEAVY_CHARGE_KM": document.getElementById('cfg_TESLA_HEAVY_CHARGE_KM').value,
                    "DISCORD_WEBHOOK_URL": document.getElementById('cfg_DISCORD_WEBHOOK_URL').value
                };
                try {
                    await fetch('/api/sys_settings', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ action: "save", settings: newSet }) });
                    alert("保存しました。");
                } catch(e) {}
            }

            async function resetSysSettings() {
                if(!confirm("すべての設定をデフォルトに戻しますか？")) return;
                try { await fetch('/api/sys_settings', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ action: "reset" }) }); update(); } catch(e) {}
            }

            
            function renderSolarPriorityWarningBox(warn) {
                const box = document.getElementById('solar-priority-warning');
                if (!box) return;

                if (!warn || !warn.active) {
                    box.style.display = 'none';
                    return;
                }

                box.style.display = 'block';
                box.classList.toggle('success', warn.level === 'success');

                const title = document.getElementById('solar-warning-title');
                const msg = document.getElementById('solar-warning-msg');
                const reasons = document.getElementById('solar-warning-reasons');

                if (title) title.textContent = warn.title || '';
                if (msg) msg.textContent = warn.message || '';
                if (reasons) {
                    reasons.innerHTML = '';
                    (warn.reasons || []).forEach(r => {
                        const li = document.createElement('li');
                        li.textContent = r;
                        reasons.appendChild(li);
                    });
                }
            }

            function renderApiControlFlow(d) {
                const tc = document.getElementById('dynamic-timeline');
                const sc = document.getElementById('flow-summary-container');
                if (!tc && !sc) return false;

                const flow = d.control_flow || [];
                const summary = d.flow_summary || null;
                if (!flow.length && !summary) return false;

                renderSolarPriorityWarningBox(d.solar_priority_warning);

                let summaryHtml = '';
                if (summary) {
                    const settings = summary.settings_summary || {};
                    const metrics = summary.metrics || {};
                    const alerts = summary.alerts || [];
                    const settingsHtml = Object.entries(settings).map(([k, v]) => `
                        <div class="flow-kv"><div class="flow-kv-label">${k}</div><div class="flow-kv-value">${v}</div></div>
                    `).join('');
                    const metricsHtml = Object.entries(metrics).map(([k, v]) => `
                        <div class="flow-kv"><div class="flow-kv-label">${k}</div><div class="flow-kv-value">${v}</div></div>
                    `).join('');
                    const alertsHtml = alerts.map(a => `<div class="flow-alert">⚠ ${a}</div>`).join('');
                    summaryHtml = `
                        <div class="flow-summary-card ${summary.phase || 'normal'}">
                            <div class="flow-summary-top">
                                <div>
                                    <div class="flow-policy">${summary.policy || '制御方針を計算中'}</div>
                                    <div class="flow-reason">${summary.reason || ''}</div>
                                </div>
                                <div class="flow-phase-badge">${summary.phase_label || ''}</div>
                            </div>
                            <div class="flow-next"><div class="flow-next-title">次に待っている処理</div><div>${summary.next_action || ''}</div></div>
                            ${alertsHtml}
                            <div class="flow-section-title">主要設定</div><div class="flow-kv-grid">${settingsHtml}</div>
                            <div class="flow-section-title">判断材料</div><div class="flow-kv-grid">${metricsHtml}</div>
                        </div>`;
                }

                let flowHtml = '';
                if (flow.length) {
                    flowHtml += `<div class="flow-section-title">予定タイムライン</div>`;
                    flowHtml += flow.map(item => `
                        <div class="flow-event ${item.level || 'info'}">
                            <div class="flow-time">${item.time || '--'}</div>
                            <div><div class="flow-title">${item.icon || '●'} ${item.title || ''}</div><div class="flow-detail">${item.detail || ''}</div><span class="flow-action">${item.action || '監視'}</span></div>
                        </div>`).join('');
                }

                if (d.read_only) {
                    const ro = `<div style="margin-bottom:12px;padding:12px;border-radius:12px;border:1px solid var(--house);background:rgba(255,69,58,.15);text-align:center;"><div style="font-weight:bold;color:var(--house);font-size:.95rem;">🔒 読み取り専用モード作動中</div><div style="font-size:.8rem;color:var(--text-sub);margin-top:4px;">全自動制御ロジックを一時的に完全凍結しています。</div></div>`;
                    summaryHtml = ro + summaryHtml;
                }

                if (sc && sc.innerHTML !== summaryHtml) sc.innerHTML = summaryHtml;
                if (tc && tc.innerHTML !== flowHtml) tc.innerHTML = flowHtml;
                return true;
            }

            function updateFlowChart(d) {
                try {
                    if (renderApiControlFlow(d)) return;
                    const now = new Date(); const h = now.getHours(); let flowHtml = '';
                    const skipBoost = d.forecast?.skip_day_boost || false;
                    const heavyCharge = d.forecast?.tesla_needs_heavy_charge || false;
                    const dayThre = d.ctrl?.day_threshold ?? '--'; const nightThre = d.ctrl?.night_threshold ?? '--';
                    const currentSoc = d.soc ?? 0;
                    const currentStatus = d.ctrl?.status ?? 'auto';
                    const maxAmps = d.sys_settings?.MAX_CHARGE_AMPS ?? 32;

                    let tLimit = d.tesla?.charge_limit || 100;
                    let tMax = d.tesla?.max_range || 400;
                    let tCur = d.tesla?.range || 0;
                    let missingKm = Math.max(0, (tMax * (tLimit / 100.0)) - tCur);
                    let isHeavyPrediction = missingKm >= (d.sys_settings?.TESLA_HEAVY_CHARGE_KM || 30);
                    
                    let nextDaySolar = d.forecast?.tomorrow_total || d.forecast?.today_total || 0;
                    let nextNightSolar = d.forecast?.tomorrow_total || d.forecast?.today_total || 0;

                    let c = h === 0 ? 24 : h;
                    let blockSequence = ['night', 'morning', 'day', 'afternoon'];
                    let currentIndex = 0;
                    currentIndex = d.phase_index ?? 0;

                    let displayBlocks = [
                        { id: blockSequence[(currentIndex - 1 + 4) % 4], state: 'done', label: '実績' },
                        { id: blockSequence[currentIndex], state: 'active', label: '実行中' },
                        { id: blockSequence[(currentIndex + 1) % 4], state: 'future', label: '予定' },
                        { id: blockSequence[(currentIndex + 2) % 4], state: 'future', label: '予定' },
                    ];

                    const getBlockContent = (id, stateStr) => {
                        let title = '', desc = '', timeStr = '';

                        if (id === 'night') {
                            timeStr = "Primary Charge Window";
                            title = "primary 最低SOC防衛 ＆ リレー";
                            if (stateStr === 'active') {
                                let remMin = Math.round((d.jit?.time_to_target_h || 0) * 60);
                                let tStr = remMin > 60 ? `${Math.floor(remMin/60)}時間${remMin%60}分` : `${remMin}分`;
                                
                                desc = `現在SOC ${currentSoc}%。<br>`;
                                if (currentSoc < nightThre) {
                                    desc += `Night Target(${nightThre}%)に向けて「Charge」を実行中。<br>※目標到達まで約 <b style="color:var(--text-main);">${tStr}</b> の見込みです。`;
                                } else {
                                    desc += `primary目標(${nightThre}%)を達成しました。`;
                                }
                            } else if (stateStr === 'future') {
                                desc = `<span style="color:var(--tesla); font-weight:bold;">【AI予測】</span>テスラの残充電枠(<b style="color:var(--text-main);">${missingKm.toFixed(1)}km</b>)と次の日の発電予測(<b style="color:var(--text-main);">${nextNightSolar.toFixed(1)}kWh</b>)を分析中。<br><br>`;
                                if (isHeavyPrediction) {
                                    desc += `⚠️テスラの充電不足しています。次のprimary window startからの格安時間帯で充電可能です。テスラ側でタイマー充電が開始されたら、グリッド枠内でアンペアを自動調整します。蓄電池も${nightThre}%まで並行充電します。`;
                                } else if (nextNightSolar < 10) {
                                    desc += `☁️次の日の太陽光があまり期待できないため、primary charge windowにTeslaへ充電することを推奨します。primaryのタイマー充電時に蓄電池と並行して効率よくグリッド充電が行われるように制御します。`;
                                } else {
                                    desc += `☀️次の日はたっぷり発電が期待できます！primaryのテスラ充電は控えめにし、日中の太陽光余剰を最大限に活用するエコ運用を予定しています。`;
                                }
                            } else {
                                desc = `直近のprimary防衛（テスラ＆蓄電池リレー）を無事に終了しました。`;
                            }
                        }
                        else if (id === 'morning') {
                            timeStr = "primary_window_end - solar_window_start";
                            title = "朝のAutoリセット ＆ 余剰待機";
                            if (stateStr === 'active') {
                                desc = `朝primary window endの自動リセット完了。[Auto] モード(${currentStatus})で稼働中。日照が強くなるまで待機しています。`;
                            } else if (stateStr === 'future') {
                                desc = `primary charge windowが明ける朝primary window end、蓄電池を通常稼働の [Auto] モードへ自動的に引き戻し、日中の太陽光発電に備えます。`;
                            } else {
                                desc = `朝primary window endの自動リセットを完了し、太陽光の余剰を待機する通常モードへ移行しました。`;
                            }
                        }
                        else if (id === 'day') {
                            timeStr = "Solar Assist Window";
                            title = "solar assist AIターゲットジャッジ";
                            if (stateStr === 'active') {
                                let targetSoc = d.forecast?.predicted_soc_window_end || dayThre;
                                let remMin = Math.round((d.jit?.time_to_target_h || 0) * 60);
                                let tStr = remMin > 60 ? `${Math.floor(remMin/60)}時間${remMin%60}分` : `${remMin}分`;

                                if (skipBoost) { desc = `買電スキップ発動中。今後の予想発電量(${d.forecast?.remaining_solar_kwh}kWh)が要求をカバー。売電漏れを防ぐため微小買電を許容しテスラへ太陽光余剰を回しています。`; }
                                else if (heavyCharge) { desc = `現在SOC ${currentSoc}%。テスラの充電不足分が大きいため、蓄電池のDay Target(${targetSoc}%)充電と並行して、テスラも上限(${maxAmps}A)までグリッド併用で急速充電しています！`; }
                                else if (currentSoc < targetSoc) { desc = `現在SOC ${currentSoc}%。Day Targetの ${targetSoc}% に到達するまで、蓄電池へ「Charge」を実行中。<br>※目標到達まで約 <b style="color:var(--text-main);">${tStr}</b> の見込みです。`; }
                                else if (currentSoc >= targetSoc) { desc = `現在SOC ${currentSoc}%。Day Target(${targetSoc}%)達成済み。ハードウェアの勝手な挙動を防ぐため、solar window endまで「Standby」で残量を一時ロックしています。`; }
                            } else if (stateStr === 'future') {
                                desc = `<span style="color:var(--solar); font-weight:bold;">【AI予測】</span>次の日の発電予測(<b style="color:var(--text-main);">${nextDaySolar.toFixed(1)}kWh</b>)とテスラの状態から判断し、<br><br>`;
                                if (nextDaySolar >= 15) {
                                    desc += `十分な発電が見込まれるため、買電をスキップして「ソーラー完全自給」に挑戦する予定です！`;
                                } else {
                                    desc += `solar assistの格安枠を使って、足りない分を効率よくグリッドから充電（AI補正）する予定です。`;
                                }
                            } else {
                                desc = `直近のsolar assist window枠のAIジャッジ・充放電コントロールを完了しました。`;
                            }
                        }
                        else if (id === 'afternoon') {
                            timeStr = "solar_window_end - primary_window_start";
                            title = "午後 SmartCharge 余剰追従";
                            if (stateStr === 'active') {
                                desc = `solar window endを過ぎ、solar assist制御によるStandbyを自動解除してAutoへ復帰しました。蓄電池の放電を誘発しない純粋な太陽光の余剰範囲内に限定して、テスラをスマートに追従充電しています。`;
                            } else if (stateStr === 'future') {
                                desc = `solar assistのジャッジ完了後、[Auto] モードに復帰し、太陽光の余剰電力だけを使ってテスラを効率よく充電する「SmartCharge」を実行します。`;
                            } else {
                                desc = `午後の余剰追従(SmartCharge)モードを終了し、primary帯へ移行しました。`;
                            }
                        }

                        return { timeStr, title, desc };
                    };

                    if (d.read_only) {
                        flowHtml = `<div style="margin-bottom: 12px; padding: 12px; border-radius: 12px; border: 1px solid var(--house); background: rgba(255, 69, 58, 0.15); text-align: center;"><div style="font-weight: bold; color: var(--house); font-size: 0.95rem;">🔒 読み取り専用モード作動中</div><div style="font-size: 0.8rem; color: var(--text-sub); margin-top: 4px;">全自動制御ロジックを一時的に完全凍結しています。</div></div>` + flowHtml;
                    }

                    displayBlocks.forEach((item) => {
                        const content = getBlockContent(item.id, item.state);
                        let colorClass = (item.state === 'active') ? 'style="color: var(--solar); border-color: var(--solar); background: rgba(255,214,10,0.1);"' : (item.state === 'done') ? 'style="color: var(--text-sub); border-color: var(--border); background: rgba(255,255,255,0.02);"' : 'style="color: var(--tesla); border-color: var(--tesla); background: rgba(50,215,75,0.1);"';
                        flowHtml += `<div style="margin-bottom: 12px; padding: 12px; border-radius: 12px; border: 1px solid var(--border); background: var(--card-bg); ${d.read_only ? 'opacity: 0.5;' : ''}"><div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;"><div style="font-family: monospace; font-size: 0.95rem; color: var(--text-main); font-weight: bold;">${content.timeStr}</div><div ${colorClass} style="padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: bold; border-width: 1px; border-style: solid;">${item.label}</div></div><div style="font-size: 0.95rem; font-weight: bold; margin-bottom: 6px; color: var(--text-main);">${content.title}</div><div style="font-size: 0.85rem; color: var(--text-sub); line-height: 1.5;">${content.desc}</div></div>`;
                    });

                    const tc = document.getElementById('dynamic-timeline');
                    if (tc && tc.innerHTML !== flowHtml) { tc.innerHTML = flowHtml; }
                } catch(e) { console.error("Flow chart error:", e); }
            }

            async function update() {
                try {
                    const res = await fetch('/api/data'); if (!res.ok) return;
                    const d = await res.json(); lastData = d; updateLowSolarProposal(d); updateFlowChart(d);

                    try {
                        if (d.sys_settings) {
                            ['SYSTEM_CAPACITY_W', 'SYSTEM_LOSS_FACTOR', 'BATT_MAX_WH', 'GRID_LIMIT_W', 'WARNING_LIMIT_W', 'CRITICAL_LIMIT_W', 'TARGET_SAFE_POWER_W', 'MAX_CHARGE_AMPS', 'TESLA_HEAVY_CHARGE_KM', 'DISCORD_WEBHOOK_URL'].forEach(k => {
                                const el = document.getElementById('cfg_' + k);
                                if (el && document.activeElement !== el) el.value = d.sys_settings[k];
                            });
                            document.getElementById('dash_sys_cap').innerText = (d.sys_settings.SYSTEM_CAPACITY_W / 1000).toFixed(1);
                        }
                    } catch(e){}

                    try {
                        if (d.logs && d.logs.length > 0) {
                            document.getElementById('log-list-container').innerHTML = d.logs.map(l => `<div class="log-item level-${l.level}"><div class="log-time">${l.time}</div><div class="log-content"><div class="log-event">${l.event}</div><div class="log-action">${l.action}</div></div></div>`).join('');
                        } else if (d.logs) { document.getElementById('log-list-container').innerHTML = '<div class="empty-log">システムログはありません。</div>'; }
                    } catch(e) {}

                    try {
                        document.getElementById('rate-label').innerText = d.rate?.label || '---';
                        document.getElementById('rate-label').className = d.rate?.is_cheap ? 'cheap-active' : '';
                        document.getElementById('rate-timer').innerText = d.rate?.timer || '--:--';
                    } catch(e) {}
                    
                    try {
                        document.getElementById('solar').innerText = ((d.solar||0)/1000).toFixed(2);
                        document.getElementById('grid').innerText = ((d.grid||0)/1000).toFixed(2);
                        document.getElementById('cons').innerText = ((d.consumption||0)/1000).toFixed(2);
                        const totalSplitForUi = d.load_source_split?.total || {};
                        const totalSourceMixEl = document.getElementById('total-source-mix');
                        if (totalSourceMixEl) {
                            totalSourceMixEl.innerText = `S ${((totalSplitForUi.solar_w||0)/1000).toFixed(2)} / B ${((totalSplitForUi.battery_w||0)/1000).toFixed(2)} / G ${((totalSplitForUi.grid_w||0)/1000).toFixed(2)} kW`;
                        }
                        document.getElementById('soc_main').innerText = d.soc || 0;
                        document.getElementById('grid_cap').innerText = ((d.grid_cap||0)/1000).toFixed(1);
                    } catch(e) {}
                    
                    try {
                        if (d.forecast && d.forecast.today_date && d.forecast.today_date !== "--/--") {
                            if (window.lastForecastCalc !== d.forecast.last_calc) {
                                window.lastForecastCalc = d.forecast.last_calc;
                                document.getElementById('tab-today').innerText = `Today (${d.forecast.today_date})`;
                                document.getElementById('tab-tomorrow').innerText = `Tomorrow (${d.forecast.tomorrow_date})`;
                                document.getElementById('forecast-content-today').innerHTML = buildHourlyHTML(d.forecast.today_hourly, d.forecast.today_total, d.sys_settings?.SYSTEM_CAPACITY_W);
                                document.getElementById('forecast-content-tomorrow').innerHTML = buildHourlyHTML(d.forecast.tomorrow_hourly, d.forecast.tomorrow_total, d.sys_settings?.SYSTEM_CAPACITY_W);
                            }
                        }
                    } catch(e) {}

                    try {
                        const elReadOnly = document.getElementById('read-only-toggle'); if (elReadOnly && elReadOnly.checked !== d.read_only) elReadOnly.checked = d.read_only;
                        if (d.settings) {
                            const elDay = document.getElementById('day-auto-toggle'); if (elDay && elDay.checked !== d.settings.day_auto_mode) elDay.checked = d.settings.day_auto_mode;
                            const elTesla = document.getElementById('tesla-auto-toggle'); if (elTesla && elTesla.checked !== d.settings.tesla_auto_mode) elTesla.checked = d.settings.tesla_auto_mode;
                            const elMinSoc = document.getElementById('tesla-min-soc-slider');
                            if (elMinSoc && document.activeElement !== elMinSoc && d.settings.tesla_min_soc !== undefined) {
                                elMinSoc.value = d.settings.tesla_min_soc; document.getElementById('tesla-min-soc-val').innerText = d.settings.tesla_min_soc + '%';
                            }
                            const elMode = document.getElementById('tesla-charge-mode-select');
                            if (elMode && document.activeElement !== elMode && d.settings.tesla_charge_mode !== undefined) elMode.value = d.settings.tesla_charge_mode;
                            const elAssist = document.getElementById('grid-assist-slider');
                            if (elAssist && document.activeElement !== elAssist && d.settings.solar_priority_grid_assist_w !== undefined) {
                                elAssist.value = d.settings.solar_priority_grid_assist_w; document.getElementById('grid-assist-val').innerText = d.settings.solar_priority_grid_assist_w + 'W';
                            }
                            const elAfSoc = document.getElementById('afternoon-target-slider');
                            if (elAfSoc && document.activeElement !== elAfSoc && d.settings.afternoon_target_soc !== undefined) {
                                elAfSoc.value = d.settings.afternoon_target_soc; document.getElementById('afternoon-target-val').innerText = d.settings.afternoon_target_soc + '%';
                            }
                            const elAfHour = document.getElementById('afternoon-hour-slider');
                            if (elAfHour && document.activeElement !== elAfHour && d.settings.afternoon_target_hour !== undefined) {
                                elAfHour.value = d.settings.afternoon_target_hour; document.getElementById('afternoon-hour-val').innerText = d.settings.afternoon_target_hour + ':00';
                            }
                            const elAfToggle = document.getElementById('afternoon-recovery-toggle');
                            if (elAfToggle && elAfToggle.checked !== d.settings.afternoon_recovery_mode) elAfToggle.checked = d.settings.afternoon_recovery_mode;
                            const afInfo = document.getElementById('afternoon-recovery-info');
                            if (afInfo && d.afternoon) {
                                afInfo.innerText = `solar window後回復判定: ${d.afternoon.can_recover_after_solar_window ? 'OK' : '不足'} / ` +
                                    `solar window end以降Solar余力 ${d.afternoon.solar_for_battery_kwh}kWh / ` +
                                    `蓄電池必要 ${d.afternoon.battery_needed_kwh}kWh / ` +
                                    `solar window end必要SOC ${d.afternoon.required_pre_solar_window_soc}%`;
                                afInfo.style.color = d.afternoon.can_recover_after_solar_window ? 'var(--tesla)' : 'var(--solar)';
                            }
                        }
                        if (d.ctrl) {
                            const nSlider = document.getElementById('night-slider'); if (nSlider && document.activeElement !== nSlider && d.ctrl.night_threshold !== undefined) { nSlider.value = d.ctrl.night_threshold; document.getElementById('night-val').innerText = d.ctrl.night_threshold + '%'; }
                            const dSlider = document.getElementById('day-slider'); if (dSlider && document.activeElement !== dSlider && d.ctrl.day_threshold !== undefined) { dSlider.value = d.ctrl.day_threshold; document.getElementById('day-val').innerText = d.ctrl.day_threshold + '%'; }
                        }
                    } catch(e) {}

                    try {
                        const bf = document.getElementById('b-flow');
                        bf.innerText = Math.abs(d.battery||0) + ' W ' + ((d.battery||0) > 0 ? 'IN' : ((d.battery||0) < 0 ? 'OUT' : ''));
                        bf.style.color = (d.battery||0) > 50 ? 'var(--tesla)' : ((d.battery||0) < -50 ? '#ff9f0a' : 'var(--text-main)');
                        const teslaWallPowerW = Math.max(0, Number(d.tesla_wall_power_w ?? d.wall_connector?.power_w ?? d.tesla?.wall_connector?.power_w ?? 0));
                        const teslaKw = teslaWallPowerW / 1000;
                        const teslaLoadType = d.wall_connector?.load_type || d.tesla?.wall_connector?.load_type || d.tesla?.charging_state || 'none';
                        const rangeStatus = d.tesla_range_status || {};
                        const teslaSoc = Number(rangeStatus.soc_percent ?? d.tesla_soc_percent ?? d.tesla?.soc ?? 0);
                        const teslaRangeKm = Number(rangeStatus.range_km ?? d.tesla_range_km ?? d.tesla?.range ?? 0);
                        const teslaMissingKm = Number(rangeStatus.missing_km ?? d.tesla_missing_km ?? 0);
                        const tSocMain = document.getElementById('t-soc-main'); if (tSocMain) tSocMain.innerText = teslaSoc > 0 ? teslaSoc.toFixed(0) : '--';
                        const tRangeKm = document.getElementById('t-range-km'); if (tRangeKm) tRangeKm.innerText = teslaRangeKm > 0 ? `${teslaRangeKm.toFixed(0)} km` : '-- km';
                        const tMissingKm = document.getElementById('t-missing-km'); if (tMissingKm) tMissingKm.innerText = teslaMissingKm >= 0 ? `${teslaMissingKm.toFixed(0)} km` : '-- km';
                        const tLoadType = document.getElementById('t-load-type'); if (tLoadType) tLoadType.innerText = teslaLoadType;
                        const tWallKw = document.getElementById('t-wall-kw'); if (tWallKw) tWallKw.innerText = teslaKw.toFixed(2);
                        const tp = document.getElementById('t-power');
                        if (tp) {
                            tp.innerText = `${teslaKw.toFixed(2)} kW`;
                            tp.style.color = teslaWallPowerW > 50 ? 'var(--tesla)' : 'var(--text-sub)';
                        }
                        const tBar = document.getElementById('tesla-soc-bar');
                        if (tBar) {
                            tBar.style.width = Math.min(100, Math.max(0, teslaSoc || 0)) + '%';
                            tBar.classList.toggle('charging-flow', teslaWallPowerW > 50);
                            const kwForSpeed = Math.min(6.4, Math.max(0.1, teslaKw));
                            const duration = Math.max(0.45, 4.0 - (kwForSpeed / 6.4) * 3.2);
                            tBar.style.setProperty('--flow-duration', duration.toFixed(2) + 's');
                        }
                    } catch(e) {}

                    try { document.getElementById('predict-area').innerHTML = (d.predicts || []).map(p => `<div class="p-item"><div class="p-time">${p.time}</div><div class="p-soc">${p.soc}%</div><div class="p-wh" style="color: #666;">${p.wh}Wh</div></div>`).join(''); } catch(e) {}
                    
                    try {
                        const syscap = d.sys_settings?.SYSTEM_CAPACITY_W || 2400; const gcap = d.sys_settings?.GRID_LIMIT_W || 6400;
                        document.getElementById('solar-bar').style.width = Math.min(100, ((d.solar||0)/syscap)*100) + '%';
                        document.getElementById('grid-bar').style.width = Math.min(100, (Math.abs(d.grid||0)/gcap)*100) + '%';
                        document.getElementById('battery-bar').style.width = (d.soc||0) + '%';
                        const totalSplit = d.load_source_split?.total || {};
                        const consSolarBar = document.getElementById('cons-solar-bar');
                        const consBatteryBar = document.getElementById('cons-battery-bar');
                        const consGridBar = document.getElementById('cons-grid-bar');
                        if (consSolarBar) consSolarBar.style.width = Math.min(100, Math.max(0, totalSplit.solar_pct || 0)) + '%';
                        if (consBatteryBar) consBatteryBar.style.width = Math.min(100, Math.max(0, totalSplit.battery_pct || 0)) + '%';
                        if (consGridBar) consGridBar.style.width = Math.min(100, Math.max(0, totalSplit.grid_pct || 0)) + '%';
                        document.getElementById('solar-bar').classList.toggle('charging-flow', (d.solar||0) > 50); document.getElementById('grid-bar').classList.toggle('charging-flow', Math.abs(d.grid||0) > 50); document.getElementById('battery-bar').classList.toggle('charging-flow', (d.battery||0) > 50);
                        if (consSolarBar) consSolarBar.classList.toggle('charging-flow', (totalSplit.solar_w||0) > 50);
                        if (consBatteryBar) consBatteryBar.classList.toggle('charging-flow', (totalSplit.battery_w||0) > 50);
                        if (consGridBar) consGridBar.classList.toggle('charging-flow', (totalSplit.grid_w||0) > 50);
                    } catch(e) {}
                } catch(e) {}
            }

            function showBatteryPrediction() {
                if (!lastData) return;
                const overlay = document.getElementById('predict-overlay'); if (overlay.style.opacity === '1') { overlay.style.opacity = '0'; return; }
                const d = lastData; const hoursLeft = Math.max(0, Number(d.jit?.hours_left || 0)); const batteryW = Math.max(0, d.battery || 0); 
                const gLimit = d.sys_settings?.GRID_LIMIT_W || 6400; const bMax = d.sys_settings?.BATT_MAX_WH || 12600;
                const postTeslaW = Math.max(0, Math.min(5000, gLimit + (d.solar||0) - (d.pure_consumption||0)));
                let addedWh = 0;
                
                if (d.tesla?.charging_state === 'Charging' && (d.tesla?.time_to_full || 0) > 0) {
                    const teslaHours = Math.min(hoursLeft, d.tesla.time_to_full); const afterTeslaHours = Math.max(0, hoursLeft - teslaHours);
                    addedWh = (batteryW * teslaHours) + (postTeslaW * afterTeslaHours);
                    document.getElementById('calc-p1').innerText = `${Math.round(batteryW)}W × ${teslaHours.toFixed(2)}h`;
                    document.getElementById('calc-avail').innerText = `Limit: ${Math.round(gLimit + (d.solar||0) - (d.pure_consumption||0))}W`;
                    document.getElementById('calc-p2').innerText = `${Math.round(postTeslaW)}W × ${afterTeslaHours.toFixed(2)}h`;
                } else {
                    addedWh = batteryW * hoursLeft;
                    document.getElementById('calc-p1').innerText = `${Math.round(batteryW)}W × ${hoursLeft.toFixed(2)}h`;
                    document.getElementById('calc-avail').innerText = `---`; document.getElementById('calc-p2').innerText = `-- W × 0.00h`;
                }
                const addedSoc = (addedWh / bMax) * 100; const predictedSoc = Math.min(100, (d.soc||0) + addedSoc);
                document.getElementById('po-soc').innerText = predictedSoc.toFixed(1);
                overlay.style.opacity = '1'; setTimeout(() => { overlay.style.opacity = '0'; }, 8000); 
            }

            async function manualFleetRefresh(evt) {
                if (evt) evt.stopPropagation();
                const btn = document.getElementById('tesla-refresh-btn');
                try {
                    if (btn) { btn.disabled = true; btn.innerText = '更新中'; }
                    const r = await fetch('/api/command', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ target: 'tesla', action: 'refresh_fleet_state' })
                    });
                    const j = await r.json();
                    if (j.status !== 'success') alert(j.message || 'Tesla更新に失敗しました。');
                    await update();
                } catch(e) {
                    alert('Tesla更新に失敗しました: ' + e);
                } finally {
                    if (btn) { btn.disabled = false; btn.innerText = '更新'; }
                }
            }

            async function sendCommand(target, action) {
                if(lastData && lastData.read_only) { alert("読み取り専用モード中は送信できません。"); return; }
                if(!confirm(`${target} を ${action} に変更しますか？`)) return;
                try {
                    const r = await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: target, action: action }) });
                    if ((await r.json()).status === "success") { container.scrollTo({ left: container.clientWidth * 4, behavior: 'smooth' }); }
                } catch(e) {}
            }
            async function sendThreshold(action, val) {
                if(lastData && lastData.read_only) { alert("読み取り専用モード中は送信できません。"); return; }
                if(!confirm(`目標を ${val}% に設定しますか？`)) return;
                try {
                    const r = await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'battery', action: action, value: val }) });
                    if ((await r.json()).status === "success") { container.scrollTo({ left: container.clientWidth * 4, behavior: 'smooth' }); }
                } catch(e) {}
            }
            async function setTeslaMinSoc(val) { try { fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_tesla_min_soc', value: val }) }); } catch(e) {} }
            async function setTeslaChargeMode(val) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_tesla_charge_mode', value: val }) }); update(); } catch(e) {} }
            async function setSolarPriorityGridAssist(val) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_solar_priority_grid_assist', value: val }) }); update(); } catch(e) {} }
            async function setAfternoonTargetSoc(val) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_afternoon_target_soc', value: val }) }); update(); } catch(e) {} }
            async function setAfternoonTargetHour(val) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_afternoon_target_hour', value: val }) }); update(); } catch(e) {} }
            async function toggleAfternoonRecovery(isChecked) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_afternoon_recovery_mode', value: isChecked }) }); update(); } catch(e) {} }
            async function toggleDayAuto(isChecked) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_day_auto', value: isChecked }) }); } catch(e) {} }
            async function toggleTeslaAuto(isChecked) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_tesla_auto', value: isChecked }) }); } catch(e) {} }
            async function toggleReadOnly(isChecked) { try { await fetch('/api/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'settings', action: 'set_read_only', value: isChecked }) }); } catch(e) {} }

            document.addEventListener('DOMContentLoaded', () => {
                setTimeout(() => { container.scrollTo({ left: container.clientWidth * 3, behavior: 'instant' }); }, 10);
            });

            setInterval(update, 2000); update();
            setInterval(() => {
                const now = new Date();
                document.getElementById('current-time').innerText = now.toLocaleTimeString('ja-JP', { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' });
                document.getElementById('current-date').innerText = now.toLocaleDateString('ja-JP', { year:'numeric', month:'short', day:'numeric', weekday:'short' });
            }, 1000);
        

        function attachFallbackSaveHandlers() {
            const bindings = [
                ["day-threshold", "battery", "thre_day"],
                ["night-threshold", "battery", "thre_night"],
                ["afternoon-target-soc", "settings", "set_afternoon_target_soc"],
                ["afternoon-target-hour", "settings", "set_afternoon_target_hour"],
                ["tesla-min-soc", "settings", "set_tesla_min_soc"],
                ["solar-priority-grid-assist", "settings", "set_solar_priority_grid_assist"],
            ];

            bindings.forEach(([id, target, action]) => {
                const el = document.getElementById(id);
                if (!el || el.dataset.fallbackSave === "1") return;
                el.dataset.fallbackSave = "1";
                el.addEventListener("change", () => {
                    markPendingControlEdit(id);
                    sendCommand(target, action, el.value);
                    setTimeout(() => clearPendingControlEdit(id), 2500);
                });
            });
        }

        document.addEventListener("DOMContentLoaded", () => {
            enhanceAllRangeControls();
            attachFallbackSaveHandlers();
        });


        setInterval(() => {
            const now = Date.now();
            Object.keys(pendingControlEdits).forEach(k => {
                if (pendingControlEdits[k] <= now) delete pendingControlEdits[k];
            });
        }, 1000);



let lastCopyOutputText = "";

function formatSettingsForCopyFixed(data) {
    const now = new Date();
    const summary = {
        battery_soc: data.soc,
        battery_power_w: data.battery,
        solar_w: data.solar,
        grid_w: data.grid,
        consumption_w: data.consumption,
        tesla_soc: data.tesla && data.tesla.soc,
        tesla_state: data.tesla && data.tesla.charging_state,
        tesla_current_a: data.tesla && (data.tesla.request_current || data.tesla.actual_current),
        battery_control_status: data.ctrl && (data.ctrl.status || data.ctrl.mode),
        tesla_charge_mode: data.settings && data.settings.tesla_charge_mode,
        day_target: (data.ctrl && data.ctrl.day_threshold) || (data.forecast && data.forecast.predicted_soc_window_end),
        night_target: data.ctrl && data.ctrl.night_threshold,
        afternoon_target_soc: data.settings && data.settings.afternoon_target_soc,
        afternoon_target_hour: data.settings && data.settings.afternoon_target_hour,
        solar_priority_grid_assist_w: data.settings && data.settings.solar_priority_grid_assist_w
    };

    const payload = {
        exported_at: now.toISOString(),
        exported_at_jst: now.toLocaleString("ja-JP"),
        summary,
        settings: data.settings || {},
        sys_settings: data.sys_settings || {},
        ctrl: data.ctrl || {},
        afternoon: data.afternoon || {},
        forecast: data.forecast || {},
        solar_priority_warning: data.solar_priority_warning || {},
        control_flow: data.control_flow || [],
        low_solar_proposal: data.low_solar_proposal || {},
        tesla: data.tesla || {},
        rate: data.rate || {}
    };

    return [
        "# HEMS Current Settings Export",
        "",
        `Exported: ${payload.exported_at_jst}`,
        "",
        "## Summary",
        `Battery SOC: ${summary.battery_soc}%`,
        `Battery Power: ${summary.battery_power_w} W`,
        `Solar: ${summary.solar_w} W`,
        `Grid: ${summary.grid_w} W`,
        `Home Consumption: ${summary.consumption_w} W`,
        `Tesla SOC: ${summary.tesla_soc}%`,
        `Tesla State: ${summary.tesla_state}`,
        `Tesla Current: ${summary.tesla_current_a} A`,
        `Battery Control: ${summary.battery_control_status}`,
        `Tesla Charge Mode: ${summary.tesla_charge_mode}`,
        `Day Target: ${summary.day_target}%`,
        `Night Target: ${summary.night_target}%`,
        `Afternoon Target: ${summary.afternoon_target_soc}% @ ${summary.afternoon_target_hour}:00`,
        `Grid Assist: ${summary.solar_priority_grid_assist_w} W`,
        "",
        "## JSON",
        JSON.stringify(payload, null, 2)
    ].join("\n");
}

async function copyTextToClipboardFixed(text) {
    if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
        return true;
    }

    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.left = "0";
    textarea.style.top = "0";
    textarea.style.width = "1px";
    textarea.style.height = "1px";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    textarea.setSelectionRange(0, text.length);

    let ok = false;
    try {
        ok = document.execCommand("copy");
    } catch (e) {
        ok = false;
    }
    document.body.removeChild(textarea);

    if (!ok) throw new Error("execCommand copy failed");
    return true;
}

function showCopyToastFixed(message) {
    const toast = document.getElementById("copy-toast");
    if (!toast) {
        alert(message);
        return;
    }
    toast.textContent = message;
    toast.classList.add("show");
    setTimeout(() => toast.classList.remove("show"), 1800);
}

function showCopyOutput(text, desc) {
    lastCopyOutputText = text;
    const modal = document.getElementById("copy-output-modal");
    const textarea = document.getElementById("copy-output-textarea");
    const description = document.getElementById("copy-output-desc");
    if (!modal || !textarea) {
        alert("コピー用テキスト表示欄が見つかりません");
        return;
    }

    textarea.value = text;
    if (description) description.textContent = desc || "自動コピーできない場合は、下の内容を選択してコピーしてください。";
    modal.classList.add("show");

    setTimeout(() => {
        textarea.focus();
        textarea.select();
        textarea.setSelectionRange(0, textarea.value.length);
    }, 100);
}

function hideCopyOutput() {
    const modal = document.getElementById("copy-output-modal");
    if (modal) modal.classList.remove("show");
}

function closeCopyOutput(event) {
    if (event.target.id === "copy-output-modal") hideCopyOutput();
}

async function retryCopyOutput() {
    if (!lastCopyOutputText) return;
    try {
        await copyTextToClipboardFixed(lastCopyOutputText);
        showCopyToastFixed("コピーしました");
    } catch (e) {
        const textarea = document.getElementById("copy-output-textarea");
        if (textarea) {
            textarea.focus();
            textarea.select();
            textarea.setSelectionRange(0, textarea.value.length);
        }
        showCopyToastFixed("手動コピーしてください");
    }
}

async function copyCurrentSettings() {
    try {
        const res = await fetch("/api/data", { cache: "no-store" });
        const data = await res.json();
        const text = formatSettingsForCopyFixed(data);
        lastCopyOutputText = text;

        try {
            await copyTextToClipboardFixed(text);
            showCopyToastFixed("現在の設定値をコピーしました");
        } catch (copyError) {
            console.warn(copyError);
            showCopyOutput(
                text,
                "ブラウザの制限で自動コピーできませんでした。下の内容は全選択済みです。Ctrl+C / ⌘C でコピーしてください。"
            );
            showCopyToastFixed("手動コピーしてください");
        }
    } catch (e) {
        console.error(e);
        showCopyToastFixed("設定値の取得に失敗しました");
    }
}



window.HELP_TEXTS = {
    hardware_limits: {
        title: "Hardware Limits & Thresholds",
        body: `この画面は、物理設備や安全制御の上限を設定します。

・太陽光パネル容量:
  発電予測の最大値計算に使います。

・システム損失係数:
  パネル理論値から実発電へ落とす補正係数です。

・蓄電池最大容量:
  SOC→kWh換算、Afternoon Target計算、残量予測に使います。

・契約グリッド上限:
  UIの買電余力表示や安全判断の基準です。

・警告ブレーカー限界:
  一定時間超えるとTesla電流を段階降下します。

・緊急ブレーカー遮断:
  超過時にTesla電流を即時降下します。

・降下ターゲット安全圏:
  Tesla電流を下げるとき、ここまで買電を落とす目標値です。

・Tesla最大許容電流:
  自動制御がTeslaへ設定する最大Aです。

・Tesla急速充電発動:
  不足kmがこの値以上なら、solar assist windowにTesla優先寄りの判断になります。`
    },
    ai_targets: {
        title: "AI / Target Settings",
        body: `・Day Target:
  solar window end時点の目安SOCです。
  solar assist windowの安価時間帯で、蓄電池へどれくらい入れるべきかの基準です。

・Afternoon Target:
  夕方までに確保したいSOCです。
  solar window end時点で低くても、午後ソーラーでここまで回復できるなら、solar assist windowはTesla優先を許容できます。

・Afternoon Target Hour:
  Afternoon Targetを達成したい時刻です。
  基本は17時。冬は16時、夏は18時でも使えます。

・Afternoon Recovery:
  solar window end以降の太陽光で蓄電池が回復できるかを見て、solar assist windowの蓄電池グリッド充電を抑制する機能です。`
    },
    tesla_mode: {
        title: "Tesla Charge Mode",
        body: `・Auto:
  従来寄りの自動制御です。
  安価時間帯ではグリッド枠も使ってTeslaを積極充電します。

・Solar Priority:
  太陽光優先です。
  solar assist windowはGrid Assistぶんだけ少量買電を許容します。
  solar window end以降は太陽光余剰以下を基本にします。
  ただし短時間の蓄電池放電はバッファとして許容します。

・Manual:
  Tesla電流を自動で触りません。
  ただしブレーカー保護は最優先で動きます。`
    },
    grid_assist: {
        title: "Grid Assist",
        body: `Solar Priority時に、solar assist windowだけ許容する少量買電の枠です。

例:
太陽光 1100W
Tesla 6A ≒ 1200W
差分 100W

Grid Assistが200Wなら、6A充電を許容します。
solar window end以降は基本的にGrid Assistを使わず、太陽光余剰以下を狙います。`
    },
    decision_flow: {
        title: "Decision Flow",
        body: `これから起きる予定の制御フローです。

固定のprimary_window_end/solar_window_start/solar_window_endだけではなく、
現在のSOC、Tesla不足量、発電予測、Afternoon Targetを使って、
実際に動きそうなイベントを表示します。

・Tesla優先可能
・蓄電池予約枠を確保
・solar window endAuto復帰
・Afternoon Target到達見込み
・不足警告

などがここに出ます。`
    },
    debug_logs: {
        title: "Debug Logs",
        body: `デバッグログは、制御判断の内部入力値を確認するための表示です。

ONにすると、約15秒ごとに以下のような情報を記録します。

・solar_w
・grid_w
・battery_w
・SOC
・Tesla状態
・Tesla電流
・Tesla Charge Mode
・蓄電池モード
・solar assist/primary判定

通常運用ではOFFで大丈夫です。
意図しない制御が出たときだけONにしてください。`
    },
    proposal_mode: {
        title: "Low Solar Proposal Mode",
        body: `低発電日向けの提案モードです。

前日夜〜当日primaryに明日の発電予測を見て、Night Target / Day Target の推奨値を表示します。

・明日発電が少ない
・solar window end以降の太陽光でAfternoon Targetへ回復できない
・Tesla不足が10km以下で自然減少レベル

このような条件では、Teslaより蓄電池を優先し、Night Targetを高めにする提案を出します。

設定は勝手に変えません。オーナーが「推奨値を適用」を押した時だけ反映します。`
    },
    copy_settings: {
        title: "設定値エクスポート",
        body: `現在の設定値をまとめてクリップボードへコピーします。

コピー内容には以下が含まれます。

・settings:
  AI制御、Tesla Charge Mode、Afternoon Target、Grid Assistなど

・sys_settings:
  ブレーカー上限、蓄電池容量、太陽光容量、Tesla最大Aなど

・ctrl:
  蓄電池制御サーバー側の現在状態、Day/Night Targetなど

・afternoon:
  solar window end以降の太陽光でAfternoon Targetへ回復できるかの判定

・forecast:
  発電予測、Day Target、Open-Meteo状態など

・tesla:
  Tesla SOC、充電状態、現在Aなど

あとでChatGPTやGeminiへ貼り付けて、設定レビューやバグ調査に使えます。`
    }
};

window.showHelp = function(key) {
    const item = window.HELP_TEXTS[key] || {
        title: "Help",
        body: "この項目の説明はまだ登録されていません。"
    };

    let modal = document.getElementById("help-modal");
    if (!modal) {
        modal = document.createElement("div");
        modal.id = "help-modal";
        modal.className = "help-modal";
        modal.onclick = closeHelp;
        modal.innerHTML = `
            <div class="help-card" onclick="event.stopPropagation()">
                <div class="help-title" id="help-title">Help</div>
                <div class="help-body" id="help-body"></div>
                <button class="help-close" onclick="hideHelp()">閉じる</button>
            </div>
        `;
        document.body.appendChild(modal);
    }

    const title = document.getElementById("help-title");
    const body = document.getElementById("help-body");
    if (title) title.textContent = item.title;
    if (body) body.textContent = item.body;
    modal.classList.add("show");
};

window.hideHelp = function() {
    const modal = document.getElementById("help-modal");
    if (modal) modal.classList.remove("show");
};

window.closeHelp = function(event) {
    if (!event || event.target.id === "help-modal") {
        hideHelp();
    }
};



window.renderLowSolarProposal = function(data) {
    const p = data.low_solar_proposal || data.proposal;
    const root =
        document.getElementById("proposal-content") ||
        document.getElementById("low-solar-proposal-content") ||
        document.getElementById("proposal-page-content") ||
        document.querySelector("[data-proposal-content]");

    if (!root) {
        console.warn("Proposal content container not found");
        return;
    }

    if (!p) {
        root.innerHTML = `
            <div class="empty-log">
                提案データが /api/data から取得できません。<br>
                /api/data の low_solar_proposal を確認してください。
            </div>`;
        return;
    }

    const level = p.level || "info";
    const activeLabel = p.active ? "提案あり" : "提案なし";
    const badgeColor = level === "critical" ? "var(--house)" : level === "warning" ? "var(--solar)" : "var(--tesla)";
    const reasons = (p.reasons || []).map(r => `<li>${r}</li>`).join("");

    root.innerHTML = `
        <div class="card" style="justify-content:flex-start; padding:14px; border-left:4px solid ${badgeColor};">
            <div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start;">
                <div>
                    <div style="font-weight:900; font-size:1.05rem;">${p.title || "低発電日提案"}</div>
                    <div style="color:var(--text-sub); font-size:.82rem; margin-top:4px; line-height:1.45;">${p.message || ""}</div>
                </div>
                <div style="font-size:.72rem; font-weight:900; color:${badgeColor}; border:1px solid ${badgeColor}; border-radius:999px; padding:4px 8px; white-space:nowrap;">
                    ${activeLabel}
                </div>
            </div>

            <div class="metric-grid" style="grid-template-columns:repeat(2,1fr); gap:8px; margin-top:12px;">
                <div class="metric-card">
                    <div class="metric-label">明日発電予測</div>
                    <div class="metric-value">${Number(p.tomorrow_total_kwh || 0).toFixed(2)}kWh</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Tesla不足</div>
                    <div class="metric-value">${Number(p.tesla_missing_km || 0).toFixed(0)}km</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Night Target</div>
                    <div class="metric-value">${p.current_night_target ?? "-"}% → ${p.recommended_night_target ?? "-"}%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Day Target</div>
                    <div class="metric-value">${p.current_day_target ?? "-"}% → ${p.recommended_day_target ?? "-"}%</div>
                </div>
            </div>

            ${reasons ? `<ul style="margin:12px 0 0 18px; color:var(--text-sub); font-size:.78rem; line-height:1.5;">${reasons}</ul>` : ""}

            <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:14px;">
                <button class="ctrl-btn" onclick="applyLowSolarProposal()" ${p.active ? "" : "disabled"}>推奨値を適用</button>
                <button class="ctrl-btn secondary" onclick="dismissLowSolarProposal()">今日は無視</button>
            </div>
        </div>
    `;
};

window.applyLowSolarProposal = async function() {
    try {
        await sendCommand("settings", "apply_low_solar_proposal");
        setTimeout(fetchData, 500);
    } catch(e) {
        console.error(e);
        alert("推奨値の適用に失敗しました");
    }
};

window.dismissLowSolarProposal = async function() {
    try {
        await sendCommand("settings", "dismiss_low_solar_proposal");
        setTimeout(fetchData, 500);
    } catch(e) {
        console.error(e);
        alert("提案の無視に失敗しました");
    }
};



window.postProposalCommand = async function(action, value=null) {
    const res = await fetch('/api/command', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            target: 'settings',
            action: action,
            value: value
        })
    });

    let data = {};
    try {
        data = await res.json();
    } catch (e) {
        data = {status: res.ok ? 'success' : 'error', message: 'JSON parse failed'};
    }

    if (!res.ok || (data.status && data.status !== 'success')) {
        throw new Error(data.message || data.error || `HTTP ${res.status}`);
    }

    return data;
};

window.applyLowSolarProposal = async function() {
    const btn = document.querySelector('.proposal-actions .apply');
    if (btn) {
        btn.disabled = true;
        btn.dataset.oldText = btn.textContent;
        btn.textContent = '適用中...';
    }

    try {
        await window.postProposalCommand('apply_low_solar_proposal');
        if (typeof showCopyToastFixed === 'function') {
            showCopyToastFixed('推奨値を適用しました');
        } else if (typeof showToast === 'function') {
            showToast('推奨値を適用しました');
        }

        setTimeout(() => {
            if (typeof fetchData === 'function') fetchData();
        }, 500);
    } catch (e) {
        console.error(e);
        alert('推奨値の設定に失敗しました: ' + e.message);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = btn.dataset.oldText || '推奨値を適用';
        }
    }
};

window.dismissLowSolarProposal = async function() {
    try {
        await window.postProposalCommand('dismiss_low_solar_proposal');
        if (typeof showCopyToastFixed === 'function') {
            showCopyToastFixed('今日の提案を無視しました');
        } else if (typeof showToast === 'function') {
            showToast('今日の提案を無視しました');
        }

        setTimeout(() => {
            if (typeof fetchData === 'function') fetchData();
        }, 500);
    } catch (e) {
        console.error(e);
        alert('提案の無視に失敗しました: ' + e.message);
    }
};



window.renderFlowSummaryVisible = function(summary) {
    const box = document.getElementById('flow-summary-container');
    if (!box) return;

    if (!summary) {
        box.innerHTML = '<div class="empty-log">Flow Summaryを計算中...</div>';
        return;
    }

    const settings = summary.settings_summary || {};
    const metrics = summary.metrics || {};
    const alerts = summary.alerts || [];

    const settingsHtml = Object.entries(settings).map(([k, v]) => `
        <div class="flow-kv">
            <div class="flow-kv-label">${k}</div>
            <div class="flow-kv-value">${v}</div>
        </div>
    `).join('');

    const metricsHtml = Object.entries(metrics).map(([k, v]) => `
        <div class="flow-kv">
            <div class="flow-kv-label">${k}</div>
            <div class="flow-kv-value">${v}</div>
        </div>
    `).join('');

    const alertsHtml = alerts.map(a => `<div class="flow-alert">⚠ ${a}</div>`).join('');

    box.innerHTML = `
        <div class="flow-summary-card ${summary.phase || 'normal'}">
            <div class="flow-summary-top">
                <div>
                    <div class="flow-policy">${summary.policy || '制御方針を計算中'}</div>
                    <div class="flow-reason">${summary.reason || ''}</div>
                </div>
                <div class="flow-phase-badge">${summary.phase_label || ''}</div>
            </div>
            <div class="flow-next">
                <div class="flow-next-title">次に待っている処理</div>
                <div>${summary.next_action || ''}</div>
            </div>
            ${alertsHtml}
            <div class="flow-section-title">主要設定</div>
            <div class="flow-kv-grid">${settingsHtml}</div>
            <div class="flow-section-title">判断材料</div>
            <div class="flow-kv-grid">${metricsHtml}</div>
        </div>
    `;
};

window.renderApiControlFlow = function(d) {
    const tc = document.getElementById('dynamic-timeline');
    if (!tc) return false;

    renderSolarPriorityWarningBox(d.solar_priority_warning);
    renderFlowSummaryVisible(d.flow_summary);
    renderTomorrowOperation(d);

    const flow = d.control_flow || [];
    if (!flow.length) {
        tc.innerHTML = '<div class="empty-log">予定タイムラインを計算中...</div>';
        return true;
    }

    let html = `<div class="flow-section-title">予定タイムライン</div>`;
    html += flow.map(item => `
        <div class="flow-event ${item.level || 'info'}">
            <div class="flow-time">${item.time || '--'}</div>
            <div>
                <div class="flow-title">${item.icon || '●'} ${item.title || ''}</div>
                <div class="flow-detail">${item.detail || ''}</div>
                <span class="flow-action">${item.action || '監視'}</span>
            </div>
        </div>
    `).join('');

    if (tc.innerHTML !== html) tc.innerHTML = html;
    return true;
};



window.renderFlowSummaryVisible = function(summary) {
    const box = document.getElementById('flow-summary-container');
    if (!box) return;

    if (!summary) {
        box.innerHTML = '<div class="flow-summary-card"><div class="flow-policy">Flow Summary未取得</div><div class="flow-reason">/api/data の flow_summary がまだありません。</div></div>';
        return;
    }

    const settings = summary.settings_summary || {};
    const metrics = summary.metrics || {};
    const alerts = summary.alerts || [];

    const settingsHtml = Object.entries(settings).map(([k, v]) => `
        <div class="flow-kv">
            <div class="flow-kv-label">${k}</div>
            <div class="flow-kv-value">${v}</div>
        </div>
    `).join('');

    const metricsHtml = Object.entries(metrics).map(([k, v]) => `
        <div class="flow-kv">
            <div class="flow-kv-label">${k}</div>
            <div class="flow-kv-value">${v}</div>
        </div>
    `).join('');

    const alertsHtml = alerts.map(a => `<div class="flow-alert">⚠ ${a}</div>`).join('');

    box.innerHTML = `
        <div class="flow-summary-card ${summary.phase || 'normal'}">
            <div class="flow-summary-top">
                <div>
                    <div class="flow-policy">${summary.policy || '制御方針を計算中'}</div>
                    <div class="flow-reason">${summary.reason || ''}</div>
                </div>
                <div class="flow-phase-badge">${summary.phase_label || ''}</div>
            </div>
            <div class="flow-next">
                <div class="flow-next-title">次に待っている処理</div>
                <div>${summary.next_action || ''}</div>
            </div>
            ${alertsHtml}
            <div class="flow-section-title">主要設定</div>
            <div class="flow-kv-grid">${settingsHtml}</div>
            <div class="flow-section-title">判断材料</div>
            <div class="flow-kv-grid">${metricsHtml}</div>
        </div>
    `;
};

window.renderApiControlFlow = function(d) {
    const tc = document.getElementById('dynamic-timeline');
    if (!tc) return false;

    if (typeof renderSolarPriorityWarningBox === 'function') {
        renderSolarPriorityWarningBox(d.solar_priority_warning);
    }

    renderFlowSummaryVisible(d.flow_summary);

    const flow = d.control_flow || [];
    if (!flow.length) {
        tc.innerHTML = '<div class="empty-log">予定タイムラインを計算中...</div>';
        return true;
    }

    let html = `<div class="flow-section-title">予定タイムライン</div>`;
    html += flow.map(item => `
        <div class="flow-event ${item.level || 'info'}">
            <div class="flow-time">${item.time || '--'}</div>
            <div>
                <div class="flow-title">${item.icon || '●'} ${item.title || ''}</div>
                <div class="flow-detail">${item.detail || ''}</div>
                <span class="flow-action">${item.action || '監視'}</span>
            </div>
        </div>
    `).join('');

    if (tc.innerHTML !== html) tc.innerHTML = html;
    return true;
};



window.renderTomorrowOperation = function(data) {
    const op = data.tomorrow_operation;
    let box = document.getElementById('tomorrow-operation-box');
    const parent = document.getElementById('flow-summary-container');
    if (!parent || !op) return;

    if (!box) {
        box = document.createElement('div');
        box.id = 'tomorrow-operation-box';
        parent.insertAdjacentElement('afterend', box);
    }

    const notes = (op.notes || []).map(n => `<li>${n}</li>`).join('');
    box.innerHTML = `
        <div class="flow-summary-card ${op.level || 'normal'}">
            <div class="flow-summary-top">
                <div>
                    <div class="flow-policy">明日の動き: ${op.title || ''}</div>
                    <div class="flow-reason">${op.policy || ''}</div>
                </div>
                <div class="flow-phase-badge">${op.forecast_date || '--/--'} / ${Number(op.tomorrow_total_kwh || 0).toFixed(1)}kWh</div>
            </div>
            <div class="flow-next">
                <div class="flow-next-title">明日の基本方針</div>
                <div>推奨Day Priority: ${op.recommended_day_priority || 'auto'}</div>
                <ul style="margin:6px 0 0 18px;">${notes}</ul>
            </div>
        </div>
    `;
};

</script>

        
<div id="copy-output-modal" class="copy-output-modal" onclick="closeCopyOutput(event)">
    <div class="copy-output-card" onclick="event.stopPropagation()">
        <div class="copy-output-title">設定値エクスポート</div>
        <div class="copy-output-desc" id="copy-output-desc">
            自動コピーできない場合は、下の内容を選択してコピーしてください。
        </div>
        <textarea id="copy-output-textarea" class="copy-output-textarea" readonly></textarea>
        <div class="copy-output-actions">
            <button class="primary" onclick="retryCopyOutput()">もう一度コピー</button>
            <button onclick="hideCopyOutput()">閉じる</button>
        </div>
    </div>
</div>

<div id="copy-toast" class="copy-toast">コピーしました</div>
        <div id="help-modal" class="help-modal" onclick="closeHelp(event)">
            <div class="help-card" onclick="event.stopPropagation()">
                <div class="help-title" id="help-title">Help</div>
                <div class="help-body" id="help-body"></div>
                <button class="help-close" onclick="hideHelp()">閉じる</button>
            </div>
        </div>

    </body>
    </html>
    """

if __name__ == '__main__':
    load_tesla_range_cache()
    threading.Thread(target=hems_worker, daemon=True).start()
    if settings.get("teslamate_mqtt_enabled", False):
        threading.Thread(target=mqtt_worker, daemon=True).start()
    else:
        print("TeslaMate MQTT worker is disabled. Code remains available, but Fleet/Wall Connector is the active source.")
    threading.Thread(target=update_forecast_worker, daemon=True).start()
    threading.Thread(target=wall_connector_worker, daemon=True).start()
    threading.Thread(target=control_worker, daemon=True).start()
    app.run(host=env_str("APP_HOST", "0.0.0.0"), port=env_int("APP_PORT", 5000))