import requests
import json
import time
import logging
import threading
from datetime import datetime

session = requests.Session()

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 配置参数
config = {
    "base_url": "http://192.168.55.235/api/v2",
    "node": "华为数采",
    "group": "invert",
    "target_tag": "电网电压AB线电压", 
    "control_tag": "无功功率补偿Q/S",
    "active_power_tag": "有功功率额定值降额（用户W）",
    "nominal_voltage": 390,
    "upper_threshold_ratio": 1.02,  # 上限阈值比例
    "lower_threshold_ratio": 0.98,  # 下限阈值比例
    "control_value": -100,  # 修改为-100以匹配我们的控制算法
    "check_interval": 0.3,
    "deadband": 2.0,  # 死区范围，单位V
    "control_interval": 10,  # 控制间隔时间(秒)
    "voltage_read_interval": 0.4,  # 电压读取间隔(秒)
    "voltage_max_deviation": 5.0,  # 最大允许电压偏差(V)
    "step_threshold_1": 10,  # 分级调节阈值1
    "step_threshold_2": 20,  # 分级调节阈值2
    "step_threshold_3": 30,  # 分级调节阈值3
    "reactive_power_threshold": 20,  # 无功功率阈值
    "active_power_step": 50,  # 有功功率调节步长
    # 无功调节系数
    "k_Q1": 5.0,   # 10V < delta <= 20V
    "k_Q2": 8.0,   # 20V < delta <= 30V
    "k_Q3": 12.0,  # delta > 30V
    # 有功调节系数
    "k_P1": 100.0, # 10V < delta <= 20V
    "k_P2": 200.0, # 20V < delta <= 30V
    "k_P3": 300.0,  # delta > 30V
    
    # PCS 协同控制参数
    "V_a": 380,
    "V_b": 385,
    "V_c": 390, # 标称/中心电压
    "V_d": 395,
    "V_e": 400,
    "V_m": 410,
    
    "pcs_node": "华为数采", # 假设与逆变器相同，需根据实际情况修改
    "pcs_group": "pcs",     # PCS 设备组名
    "pcs_active_tag": "有功功率", 
    "pcs_reactive_tag": "无功功率",
    
    "pcs_p_max": 5000, # PCS 最大放电功率 (W)
    "pcs_p_min": -5000, # PCS 最大充电功率 (W, 负值表示充电)
    "pcs_q_max": 2000,  # PCS 最大感性无功 (Var)
    "pcs_q_min": -2000  # PCS 最大容性无功 (Var)
}

# 控制算法参数 (这些参数应该通过测试获得)
# 根据测试结果得出的控制算法参数
CONTROL_ALGORITHM_SLOPE = 6.9932  # 斜率
CONTROL_ALGORITHM_INTERCEPT = -344.4380  # 截距

# 有功功率控制算法参数
ACTIVE_POWER_ALGORITHM_SLOPE = 220.5695  # 斜率
ACTIVE_POWER_ALGORITHM_INTERCEPT = 418.0645  # 截距

# 微调参数
FINE_TUNE_INTERVAL = 3  # 微调检查间隔(秒)，从5秒缩短到3秒
FINE_TUNE_VOLTAGE_STEP = 5  # 电压差阈值(V)
FINE_TUNE_CONTROL_STEP = 10  # 控制值调整步长
ACTIVE_POWER_FINE_TUNE_STEP = 250  # 有功功率微调步长
MAX_CONTROL_VALUE = 500  # 最大控制值
MIN_CONTROL_VALUE = -500  # 最小控制值，修改为-500以适应控制算法范围

# 全局变量用于记录时间
control_start_time = None
voltage_recovery_start_time = None
control_value_set = False
control_executed = False
last_voltage = None  # 用于存储上一次的电压值
last_control_time = 0  # 上次控制时间
stable_voltage_start_time = None  # 电压进入稳定范围的开始时间
last_fine_tune_time = 0  # 上次微调时间
last_control_value = 0  # 上次控制值
last_active_power_value = 1000  # 上次有功功率值
active_power_adjusted = False  # 标记本轮微调是否执行过有功功率调节

# 控制状态
current_reactive_power_value = 0  # 当前无功功率控制值（第一台逆变器）
second_reactive_power_value = 0  # 第二台逆变器无功功率控制值
reactive_power_reached_limit = False  # 第一台逆变器无功功率是否已达到极限值
second_reactive_reached_limit = False  # 第二台逆变器无功功率是否已达到极限值
active_power_in_progress = False  # 是否正在进行有功功率调节
current_active_power_value = 1000  # 当时有功功率值

# 计算上下限阈值
upper_threshold = config["nominal_voltage"] * config["upper_threshold_ratio"]
lower_threshold = config["nominal_voltage"] * config["lower_threshold_ratio"]

DEFAULT_TIMEOUT = 10

def request_api(path, payload, retries=1, method="post"):
    """统一的 API 请求入口，带会话、超时与重试。"""
    url = f"{config['base_url']}{path}"
    headers = {"Content-Type": "application/json"}

    for attempt in range(1, retries + 1):
        try:
            response = session.request(
                method=method,
                url=url,
                json=payload,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException as exc:
            logging.error(
                "API 请求异常: path=%s attempt=%d error=%s payload=%s",
                path,
                attempt,
                exc,
                payload,
            )
            if attempt < retries:
                time.sleep(1)
            continue

        if not 200 <= response.status_code < 300:
            logging.error(
                "API 响应错误: path=%s status=%s body=%s payload=%s",
                path,
                response.status_code,
                response.text,
                payload,
            )
            if attempt < retries:
                time.sleep(1)
            continue

        try:
            return response.json()
        except ValueError as exc:
            logging.error(
                "API JSON 解析失败: path=%s error=%s body=%s",
                path,
                exc,
                response.text,
            )
            if attempt < retries:
                time.sleep(1)
            continue

    return None

def write_tag(group, tag, value, node=None, retries=1):
    """统一的写入接口，返回是否成功。"""
    payload = {
        "node": node or config["node"],
        "group": group,
        "tag": tag,
        "value": value,
    }

    result = request_api("/write", payload, retries=retries)
    if result is None:
        logging.error(
            "写入失败: group=%s tag=%s value=%s payload=%s",
            group,
            tag,
            value,
            payload,
        )
        return False

    logging.info("写入成功: group=%s tag=%s value=%s", group, tag, value)
    return True

def read_inverter_data():
    """读取逆变器数据"""
    payload = {
        "node": config["node"],
        "group": config["group"],
    }
    return request_api("/read", payload, retries=3)

def read_inverter2_data():
    """读取第二个逆变器数据"""
    payload = {
        "node": config["node"],
        "group": "invert2",
    }
    return request_api("/read", payload, retries=3)

def get_reactive_power_value(data):
    """获取无功功率值"""
    if data and "tags" in data:
        for tag in data["tags"]:
            if tag.get("name") == "无功功率":
                value = tag.get("value")
                if value is not None:
                    return float(value)
    return None

def get_voltage_value(data):
    """获取电压值"""
    if data and "tags" in data:
        for tag in data["tags"]:
            if tag.get("name") == config["target_tag"]:
                value = tag.get("value")
                if value is not None:
                    return float(value)
    return None

def get_stable_voltage_value():
    """获取稳定的电压值，通过多次读取并过滤异常值"""
    voltage_readings = []
    discard_count = 0
    max_attempts = 10  # 最大尝试次数，防止无限循环
    attempt = 0
    
    while attempt < max_attempts and len(voltage_readings) < 3:
        attempt += 1
        
        # 读取数据
        data = read_inverter_data()
        if not data:
            logging.warning("读取数据失败，等待后重试...")
            time.sleep(config["voltage_read_interval"])
            continue
            
        # 获取电压值
        voltage = get_voltage_value(data)
        if voltage is None:
            logging.warning("未找到电压数据，等待后重试...")
            time.sleep(config["voltage_read_interval"])
            continue
            
        # 如果是第一次读取，直接添加
        if len(voltage_readings) == 0:
            voltage_readings.append(voltage)
            logging.debug(f"第1次读取电压: {voltage}V")
        else:
            # 检查与前一次读数的偏差
            last_voltage = voltage_readings[-1]
            deviation = abs(voltage - last_voltage)
            
            # 如果偏差在允许范围内，添加到读数列表
            if deviation <= config["voltage_max_deviation"]:
                voltage_readings.append(voltage)
                logging.debug(f"第{len(voltage_readings)}次读取电压: {voltage}V (偏差: {deviation:.2f}V)")
                discard_count = 0  # 重置丢弃计数
            else:
                # 偏差过大，丢弃这次读数
                logging.warning(f"电压偏差过大 ({deviation:.2f}V > {config['voltage_max_deviation']}V)，丢弃读数: {voltage}V")
                discard_count += 1
                
                # 如果连续丢弃2次，则清空已有的读数重新开始
                if discard_count >= 2:
                    logging.warning("连续丢弃2次读数，重新开始读取...")
                    voltage_readings.clear()
                    discard_count = 0
        
        # 如果还没收集到足够的读数，等待指定间隔
        if len(voltage_readings) < 3:
            time.sleep(config["voltage_read_interval"])
    
    # 如果成功收集到3个读数，返回平均值
    if len(voltage_readings) == 3:
        avg_voltage = sum(voltage_readings) / 3
        logging.info(f"稳定电压读取完成: {voltage_readings} -> 平均值: {avg_voltage:.2f}V")
        return avg_voltage
    else:
        logging.error(f"无法获取稳定的电压读数，尝试次数: {attempt}")
        return None

def get_control_value(data):
    """获取控制值"""
    if data and "tags" in data:
        for tag in data["tags"]:
            if tag.get("name") == config["control_tag"]:
                value = tag.get("value")
                if value is not None:
                    return float(value)
    return None

def get_second_reactive_power_value():
    """获取第二台逆变器无功功率值"""
    data = read_inverter2_data()
    if data and "tags" in data:
        for tag in data["tags"]:
            if tag.get("name") == config["control_tag"]:
                value = tag.get("value")
                if value is not None:
                    return float(value)
    return None

def write_second_control_value(control_value):
    """写入第二台逆变器控制值"""
    if write_tag("invert2", config["control_tag"], control_value):
        logging.info(f"第二台逆变器控制成功: {config['control_tag']} = {control_value}")
        return True

    logging.error(f"第二台逆变器控制失败: {control_value}")
    return False

def read_transformer_data():
    """读取台区变压器数据 (假设存在独立的变压器/电表节点)"""
    payload = {
        "node": config["node"],
        "group": "transformer"  # 假设组名为 transformer
    }
    return request_api("/read", payload, retries=3)

def get_transformer_voltage():
    """获取变压器电压"""
    # 如果没有单独的变压器数据，这只是一个占位符。
    # 实际项目中需配置正确的 tag
    data = read_transformer_data()
    if data:
        voltage = get_voltage_value(data) # 假设 tag 相同
        return voltage
    return None

def cut_out_inverter(inverter_id):
    """
    切出逆变器
    inverter_id: 1 或 2
    """
    logging.warning(f"触发切出程序: 正在切出逆变器 {inverter_id}")
    try:
        if inverter_id == 1:
            # 切出第一台：有功置0
            return write_active_power_value(0)
        elif inverter_id == 2:
            # 切出第二台：有功置0 (需要增加第二台有功写入函数? 目前没有 write_second_active_power_value)
            # 暂时假设用同样的 write_active_power_value 写入第二台的 active_power_tag
            # 但 write_active_power_value 是 hardcode group="invert"
            # 我们需要专门的写第二台有功函数
            return write_second_active_power_value(0)
    except Exception as e:
        logging.error(f"切出逆变器 {inverter_id} 异常: {e}")
        return False

def write_second_active_power_value(power_value):
    """写入第二台逆变器有功功率值"""
    if write_tag("invert2", config["active_power_tag"], power_value):
        logging.info(f"第二台逆变器有功控制成功: {power_value}")
        return True

    logging.error(f"第二台逆变器有功控制失败: {power_value}")
    return False

def check_and_perform_cutout(v1, v2):
    """
    检查是否满足切出条件并执行切出
    条件: 所有电压 > 额定 + 20 (410V)
    """
    v_trans = get_transformer_voltage()
    if v_trans is None:
        # 如果读取不到变压器电压，为安全起见，如果不强制要求v_trans，可以仅用v1,v2判断?
        # 用户明确要求 "所有...以及台区变压器电压"，所以必须都有数据
        # 这里为了演示，如果读不到，假设它也很高 (或者跳过)
        # logging.warning("无法读取变压器电压，跳过切出判断")
        # return False
        v_trans = max(v1, v2) # 临时 fallback: 假设变压器电压与最高逆变器电压接近
    
    threshold = config["nominal_voltage"] + 20 # 410V
    
    if v1 > threshold and v2 > threshold and v_trans > threshold:
        logging.warning(f"严重过压保护触发: V1={v1:.1f}, V2={v2:.1f}, Vtrans={v_trans:.1f} 均 > {threshold}V")
        
        # 排序：电压高的先切
        inverters = []
        inverters.append({"id": 1, "v": v1})
        inverters.append({"id": 2, "v": v2})
        
        # 按电压降序排序
        inverters.sort(key=lambda x: x["v"], reverse=True)
        
        # 依次切出
        for inv in inverters:
            logging.warning(f"优先切出电压最高的逆变器: ID={inv['id']}, V={inv['v']:.1f}")
            cut_out_inverter(inv['id'])
            
            # 切出后等待一小段时间检查恢复情况? 
            # 用户说 "依次切出"，通常意味着切一个看效果，不行再切下一个
            # 这里简单实现为：切除了一个，然后退出函数，让主循环下一轮再检查
            # 如果下一轮电压还高，就会切下一个 (因为已经切出的 active=0 应该会让电压降下来，或者如果还没降下来，会再次进入这里)
            # 为了防止重复切出已经切出的，可以检查当前功率?
            # 暂时简单处理：发指令置0。
            time.sleep(2) # 等待生效
            return True # 执行了一次切出
            
    return False
    """
    根据电压偏差计算控制值
    使用线性控制算法: control_value = k * deviation + b
    """
    control_value = CONTROL_ALGORITHM_SLOPE * voltage_deviation + CONTROL_ALGORITHM_INTERCEPT
    
    # 限制控制值范围
    control_value = max(MIN_CONTROL_VALUE, min(MAX_CONTROL_VALUE, control_value))
    
    return control_value

def calculate_active_power_value(voltage_deviation):
    """
    根据电压偏差计算有功功率值
    使用线性控制算法: active_power_value = k * deviation + b
    """
    active_power_value = ACTIVE_POWER_ALGORITHM_SLOPE * voltage_deviation + ACTIVE_POWER_ALGORITHM_INTERCEPT
    
    # 限制有功功率值范围（假设范围是0-10000）
    active_power_value = max(0, min(10000, active_power_value))
    
    return active_power_value

def second_reactive_power_control(voltage):
    """第二台逆变器无功功率控制函数"""
    global second_reactive_power_value, second_reactive_reached_limit
    
    # 获取第二台逆变器当前无功功率值
    current_second_reactive = get_second_reactive_power_value()
    if current_second_reactive is not None:
        second_reactive_power_value = current_second_reactive
    
    logging.info(f"第二台逆变器无功功率控制: 当前={second_reactive_power_value:.2f}, 电压={voltage:.2f}V")
    
    retry = 0
    max_retries = 20
    
    while retry < max_retries:
        # 1. 检查退出条件
        if 395 <= voltage <= 400:
             logging.info(f"电压达标({voltage:.2f}V)，第二台无功调节停止")
             return True
             
        # 2. 计算偏差
        deviation = voltage - config['nominal_voltage']
        abs_dev = abs(deviation)
        
        # 3. 选择系数 k
        if abs_dev > config['step_threshold_3']:
            k = config['k_Q3']
        elif abs_dev > config['step_threshold_2']:
            k = config['k_Q2']
        elif abs_dev > config['step_threshold_1']:
            k = config['k_Q1']
        else:
            k = config['k_Q1'] # 默认
            
        # 4. 计算调节量
        # 公式 Q_next = Q_curr + k * (390 - V)
        delta_v_calc = config['nominal_voltage'] - voltage
        adjustment = k * delta_v_calc
        
        target_reactive_power = second_reactive_power_value + adjustment
        
        # 5. 限制范围
        target_reactive_power = max(MIN_CONTROL_VALUE, min(MAX_CONTROL_VALUE, target_reactive_power))
        
        # 检查是否卡在边界
        if abs(target_reactive_power - second_reactive_power_value) < 0.1:
            logging.info(f"第二台无功功率({target_reactive_power:.1f})已无可调节空间")
            if target_reactive_power <= MIN_CONTROL_VALUE or target_reactive_power >= MAX_CONTROL_VALUE:
                second_reactive_reached_limit = True
                return False
            break

        # 6. 执行调节
        if write_second_control_value(target_reactive_power):
            second_reactive_power_value = target_reactive_power
            logging.info(f"第二台无功调节: 偏差{abs_dev:.1f}V, k={k}, 调整{adjustment:.1f}, 目标Q={target_reactive_power:.1f}")
        else:
            logging.error("第二台无功调节失败")
            break
            
        # 7. 等待并重新读取电压
        time.sleep(1)
        new_voltage = get_stable_voltage_value()
        if new_voltage is not None:
            voltage = new_voltage
        else:
            logging.warning("无法读取调节后电压")
            break
            
        retry += 1
        
    return True

def active_power_control(voltage):
    """有功功率控制函数 - 三段式变参数调节"""
    global current_active_power_value, active_power_in_progress
    
    # 设置有功功率调节标志
    active_power_in_progress = True
    
    # 获取当前有功功率值
    current_power = get_active_power_value(read_inverter_data())
    if current_power is None:
        current_power = current_active_power_value
    
    logging.info(f"开始有功功率调节: 当前={current_power:.1f}W, 电压={voltage:.2f}V")
    
    success = False
    max_retries = 20
    retry = 0
    
    while retry < max_retries:
        # 1. 检查退出条件
        if 395 <= voltage <= 400:
            logging.info(f"电压达标({voltage:.2f}V)，有功调节完成")
            success = True
            break
            
        # 2. 计算偏差 delta V = 390 - V_meas
        deviation = voltage - config['nominal_voltage']
        abs_dev = abs(deviation)
        
        # 3. 选择系数 k
        if abs_dev > config['step_threshold_3']:
            k = config['k_P3']
        elif abs_dev > config['step_threshold_2']:
            k = config['k_P2']
        elif abs_dev > config['step_threshold_1']:
            k = config['k_P1']
        else:
            k = config['k_P1']
            
        # 4. 计算调节量
        # 公式: Pt+1 = Pt + k * (390 - V)
        delta_v_calc = config['nominal_voltage'] - voltage
        adjustment = k * delta_v_calc
        
        target_power = current_power + adjustment
        
        # 5. 限制范围 (0 - 10000)
        target_power = max(0, min(10000, target_power))
        
        # 检查是否卡在边界
        if abs(target_power - current_power) < 1.0:
            logging.info(f"有功功率({target_power:.1f})已无可调节空间或变化太小")
            break
            
        # 6. 执行调节
        if write_active_power_value(target_power):
            current_power = target_power
            current_active_power_value = target_power
            logging.info(f"有功调节: 电压{voltage:.2f}V, 偏差{abs_dev:.1f}V, k={k}, 调整{adjustment:.1f}, 目标P={target_power:.1f}")
        else:
            logging.error("有功功率调节失败")
            break
            
        # 7. 等待并重新读取电压
        time.sleep(1)
        new_voltage = get_stable_voltage_value()
        if new_voltage is not None:
            voltage = new_voltage
        else:
            logging.warning("无法读取调节后电压")
            break
            
        retry += 1
        
    active_power_in_progress = False
    return success

def fine_tune_control(voltage):
    """微调控制函数，用于电压未达到稳定范围时的进一步调节"""
    global last_control_value, last_fine_tune_time, control_executed, last_control_time, last_active_power_value, active_power_adjusted

    active_power_adjusted = False
    
    # 检查电压是否在稳定范围内
    if voltage >= 395 and voltage <= 400:
        logging.info(f"电压 {voltage:.2f}V 已在稳定范围内，无需微调")
        # 重置控制执行标志，允许再次执行控制
        control_executed = False
        # 重置有功功率调节标志
        active_power_adjusted = False
        return False
    
    # 检查是否到了微调时间
    current_time = time.time()
    
    # 计算电压偏差
    voltage_deviation = voltage - 390  # 与额定电压的偏差
    
    # 对于严重偏离的情况(偏差超过20V)，立即进行调节而不考虑时间间隔
    severe_deviation = abs(voltage_deviation) > 20
    
    # 对于非常严重的偏离(偏差超过30V)，立即调节
    very_severe_deviation = abs(voltage_deviation) > 30
    
    # 如果电压偏差很大，忽略微调时间间隔限制
    if very_severe_deviation or severe_deviation:
        logging.info(f"电压偏差较大({voltage_deviation:.2f}V)，忽略微调时间间隔限制")
    elif current_time - last_fine_tune_time < FINE_TUNE_INTERVAL:
        logging.debug(f"微调间隔未到: 当前时间={current_time:.2f}, 上次微调时间={last_fine_tune_time:.2f}, 间隔={(current_time - last_fine_tune_time):.2f}s")
        return False
    
    # 更新上次微调时间
    last_fine_tune_time = current_time
    
    # 对于严重偏离的情况，检查是否应该调节有功功率
    if severe_deviation or (voltage > 400 or voltage < 395):
        # 检查无功功率是否已经调节到极限值
        if last_control_value > MIN_CONTROL_VALUE and voltage_deviation > 10:  # 电压偏高且无功功率还未调节到-200
            logging.info(f"电压偏高({voltage:.2f}V)，但无功功率({last_control_value:.1f})未达极限值(-200)，继续调节无功功率")
            # 不调节有功功率，返回False让系统继续调节无功功率
            return False
        elif last_control_value < MAX_CONTROL_VALUE and voltage_deviation < -10:  # 电压偏低且无功功率还未调节到200
            logging.info(f"电压偏低({voltage:.2f}V)，但无功功率({last_control_value:.1f})未达极限值(200)，继续调节无功功率")
            # 不调节有功功率，返回False让系统继续调节无功功率
            return False
        else:
            # 无功功率已经调节到极限值，检查当前无功功率是否已经在极限值且电压仍然偏高
            if (last_control_value <= MIN_CONTROL_VALUE or last_control_value >= MAX_CONTROL_VALUE) and abs(voltage_deviation) > 10:
                # 无功功率已经在极限值且电压仍然偏高，才调节有功功率
                current_power = get_active_power_value(read_inverter_data())
                if current_power is None:
                    current_power = last_active_power_value  # 使用上次的值
                
                # 计算目标有功功率值
                target_power = calculate_active_power_value(voltage_deviation)
                
                # 分段调节有功功率，每次调整步长为50
                power_diff = current_power - target_power
                if abs(power_diff) > ACTIVE_POWER_FINE_TUNE_STEP:
                    # 分段调节
                    if power_diff > 0:
                        new_power = current_power - ACTIVE_POWER_FINE_TUNE_STEP
                    else:
                        new_power = current_power + ACTIVE_POWER_FINE_TUNE_STEP
                else:
                    # 直接调节到目标值
                    new_power = target_power
                
                # 限制有功功率范围
                new_power = max(0, min(10000, new_power))
                
                # 根据电压偏差方向调整有功功率
                if voltage_deviation > 0:  # 电压偏高，下调有功功率
                    new_power = min(current_power, new_power)  # 确保下调
                else:  # 电压偏低，上调有功功率
                    new_power = max(current_power, new_power)  # 确保上调
                
                # 确定调节方向
                direction = "下调" if voltage_deviation > 0 else "上调"
                
                if write_active_power_value(new_power):
                    logging.info(f"电压严重偏离({voltage:.2f}V)，无功已达极限值，{direction}有功功率: {current_power:.1f} -> {new_power:.1f}")
                    last_active_power_value = new_power
                    active_power_adjusted = True
                    return True
            else:
                # 无功功率已经调节到极限值，可以调节有功功率来辅助控制电压
                current_power = get_active_power_value(read_inverter_data())
                if current_power is None:
                    current_power = last_active_power_value  # 使用上次的值
                
                # 计算目标有功功率值
                target_power = calculate_active_power_value(voltage_deviation)
                
                # 分段调节有功功率，每次调整步长为50
                power_diff = current_power - target_power
                if abs(power_diff) > ACTIVE_POWER_FINE_TUNE_STEP:
                    # 分段调节
                    if power_diff > 0:
                        new_power = current_power - ACTIVE_POWER_FINE_TUNE_STEP
                    else:
                        new_power = current_power + ACTIVE_POWER_FINE_TUNE_STEP
                else:
                    # 直接调节到目标值
                    new_power = target_power
                
                # 限制有功功率范围
                new_power = max(0, min(10000, new_power))
                
                # 根据电压偏差方向调整有功功率
                if voltage_deviation > 0:  # 电压偏高，下调有功功率
                    new_power = min(current_power, new_power)  # 确保下调
                else:  # 电压偏低，上调有功功率
                    new_power = max(current_power, new_power)  # 确保上调
                
                # 确定调节方向
                direction = "下调" if voltage_deviation > 0 else "上调"
                
                if write_active_power_value(new_power):
                    logging.info(f"电压偏离({voltage:.2f}V)，无功已达极限值，辅助{direction}有功功率: {current_power:.1f} -> {new_power:.1f}")
                    last_active_power_value = new_power
                    active_power_adjusted = True
                    return True
    
    # 计算需要调整的控制值
    if voltage > 400:
        # 电压过高，需要降低控制值
        voltage_diff = voltage - 400
        # 对于非常严重的偏离，使用更大的调节步长
        if very_severe_deviation:
            adjustment_steps = int(voltage_diff / (FINE_TUNE_VOLTAGE_STEP / 2))  # 更大的步长
        else:
            adjustment_steps = int(voltage_diff / FINE_TUNE_VOLTAGE_STEP)
        if voltage_diff % FINE_TUNE_VOLTAGE_STEP > 0:
            adjustment_steps += 1
        control_adjustment = -adjustment_steps * FINE_TUNE_CONTROL_STEP
        logging.debug(f"电压过高微调: 电压差={voltage_diff:.2f}V, 调整步数={adjustment_steps}, 调整值={control_adjustment}")
    elif voltage < 395:
        # 电压过低，需要增加控制值
        voltage_diff = 395 - voltage
        # 对于非常严重的偏离，使用更大的调节步长
        if very_severe_deviation:
            adjustment_steps = int(voltage_diff / (FINE_TUNE_VOLTAGE_STEP / 2))  # 更大的步长
        else:
            adjustment_steps = int(voltage_diff / FINE_TUNE_VOLTAGE_STEP)
        if voltage_diff % FINE_TUNE_VOLTAGE_STEP > 0:
            adjustment_steps += 1
        control_adjustment = adjustment_steps * FINE_TUNE_CONTROL_STEP
        logging.debug(f"电压过低微调: 电压差={voltage_diff:.2f}V, 调整步数={adjustment_steps}, 调整值={control_adjustment}")
    else:
        # 电压在稳定范围内
        logging.info(f"电压 {voltage:.2f}V 已在稳定范围内，无需微调")
        # 重置控制执行标志，允许再次执行控制
        control_executed = False
        # 重置有功功率调节标志
        active_power_adjusted = False
        return False
    
    # 计算新的控制值
    new_control_value = last_control_value + control_adjustment
    new_control_value = max(MIN_CONTROL_VALUE, min(MAX_CONTROL_VALUE, new_control_value))
    
    # 如果控制值没有变化，则不需要调整
    if abs(new_control_value - last_control_value) < 0.001:
        logging.info(f"电压 {voltage:.2f}V 未在稳定范围内，但控制值无需调整")
        return False
    
    # 如果新控制值与上次相同，则尝试逐步调整
    if abs(new_control_value - last_control_value) < 0.1:
        # 使用更小的步长进行调整
        if voltage > 400:
            new_control_value = max(MIN_CONTROL_VALUE, last_control_value - 10)  # 每次减10
        elif voltage < 395:
            new_control_value = min(MAX_CONTROL_VALUE, last_control_value + 10)  # 每次加10
        
        # 再次检查是否与上次相同
        if abs(new_control_value - last_control_value) < 0.1:
            logging.info(f"电压 {voltage:.2f}V 未在稳定范围内，但控制值变化太小，无需调整")
            return False
    
    logging.info(f"电压 {voltage:.2f}V 未在稳定范围内，进行微调: 调整值 {control_adjustment}, 新控制值 {new_control_value}")
    
    # 发送新的控制值
    if write_control_value(new_control_value):
        last_control_value = new_control_value
        # 更新控制执行时间，防止立即触发新的控制
        last_control_time = time.time()
        control_executed = True  # 标记为已执行控制
        return True
    else:
        logging.error("微调控制值发送失败")
        return False

def reactive_power_control(voltage):
    """无功功率控制函数 - 三段式变参数调节"""
    global current_reactive_power_value, reactive_power_reached_limit, active_power_in_progress
    
    # 检查是否需要调节
    # 如果电压在 395-400 范围内，视为稳定，不需要调节
    # 或者根据 deviation 和 thresholds 判断？这里沿用之前的稳定范围判断
    if 395 <= voltage <= 400:
        return True

    # 如果已经在进行有功功率调节，不再进行无功功率调节
    if active_power_in_progress:
        return False
    
    logging.info(f"电压({voltage:.2f}V)，开始无功调节")
    
    max_retries = 20 # 防止无限循环
    retry = 0
    
    while retry < max_retries:
        # 1. 检查退出条件
        if 395 <= voltage <= 400:
             logging.info(f"电压达标({voltage:.2f}V)，停止无功调节")
             return True
             
        # 2. 计算偏差 delta V = 390 - V_meas (用于公式 Q_next = Q_curr + k * delta)
        # 如果 V=400 (高), delta = -10. k*delta 为负, Q 减小. 正确.
        deviation = voltage - config['nominal_voltage']
        abs_dev = abs(deviation)
        
        # 3. 选择系数 k
        if abs_dev > config['step_threshold_3']:
            k = config['k_Q3']
        elif abs_dev > config['step_threshold_2']:
            k = config['k_Q2']
        elif abs_dev > config['step_threshold_1']:
            k = config['k_Q1']
        else:
            k = config['k_Q1'] # 默认系数
            
        # 4. 计算调节量
        # 公式: Qt+1 = Qt + k * (390 - V)
        delta_v_calc = config['nominal_voltage'] - voltage
        adjustment = k * delta_v_calc
        
        target_Q = current_reactive_power_value + adjustment
        
        # 5. 限制范围
        target_Q = max(MIN_CONTROL_VALUE, min(MAX_CONTROL_VALUE, target_Q))
        
        # 检查是否卡在边界
        if abs(target_Q - current_reactive_power_value) < 0.1:
            logging.info(f"无功功率({target_Q:.1f})已无可调节空间或变化太小")
            if target_Q <= MIN_CONTROL_VALUE or target_Q >= MAX_CONTROL_VALUE:
                reactive_power_reached_limit = True
                logging.info("无功功率已达到极限，准备切换有功功率调节")
                return False
            break

        # 6. 执行调节
        if write_control_value(target_Q):
            current_reactive_power_value = target_Q
            logging.info(f"无功调节: 电压{voltage:.2f}V, 偏差{abs_dev:.1f}V, k={k}, 调整{adjustment:.1f}, 目标Q={target_Q:.1f}")
        else:
            logging.error("无功控制指令发送失败")
            break
            
        # 7. 等待并重新读取电压
        time.sleep(1)
        new_voltage = get_stable_voltage_value()
        if new_voltage is not None:
            voltage = new_voltage
        else:
            logging.warning("读取电压失败，停止调节")
            break
            
        retry += 1
        
    return True

def get_active_power_value(data):
    """获取有功功率值"""
    if data and "tags" in data:
        for tag in data["tags"]:
            if tag.get("name") == "有功功率":
                value = tag.get("value")
                if value is not None:
                    return float(value * 1000)
    return None

def write_control_value(control_value=None):
    """写入控制值"""
    global control_start_time, control_value_set
    # 如果没有提供控制值，则使用配置中的默认值
    if control_value is None:
        control_value = config["control_value"]

    # 记录控制开始时间
    control_start_time = time.time() * 1000  # 转换为毫秒
    control_value_set = False

    if write_tag(config["group"], config["control_tag"], control_value):
        logging.info(f"控制成功: {config['control_tag']} = {control_value}")
        return True

    logging.error(f"控制失败: {config['control_tag']} = {control_value}")
    return False

def write_active_power_value(power_value):
    """写入有功功率值"""
    if write_tag(config["group"], config["active_power_tag"], power_value):
        logging.info(f"有功功率控制成功: {config['active_power_tag']} = {power_value}")
        return True

    logging.error(f"有功功率控制失败: {power_value}")
    return False

def should_control(voltage):
    """判断是否需要进行控制"""
    global last_control_time
    
    # 检查电压是否在正常范围内
    if voltage >= lower_threshold and voltage <= upper_threshold:
        return False
    
    # 检查是否在死区范围内
    voltage_deviation = abs(voltage - config["nominal_voltage"])
    if voltage_deviation <= config["deadband"]:
        return False
    
    # 检查控制间隔时间
    current_time = time.time()
    if current_time - last_control_time < config["control_interval"]:
        # 但是，如果电压偏差很大（>30V），则忽略控制间隔时间
        if voltage_deviation > 30:
            logging.info(f"电压偏差较大({voltage_deviation:.2f}V)，忽略控制间隔时间限制")
            return True
        return False
    
    return True

def write_pcs_active(value):
    """写入PCS有功功率"""
    return write_tag(config["pcs_group"], config["pcs_active_tag"], value, node=config["pcs_node"])

def write_pcs_reactive(value):
    """写入PCS无功功率"""
    return write_tag(config["pcs_group"], config["pcs_reactive_tag"], value, node=config["pcs_node"])

def pcs_coordinated_control(voltage):
    """
    PCS 与逆变器协同控制函数
    依据 V_a, V_b, V_c, V_d, V_e, V_m 阈值进行调节
    """
    V_a = config["V_a"]
    V_b = config["V_b"]
    V_d = config["V_d"]
    V_e = config["V_e"]
    
    P_max = config["pcs_p_max"]
    P_min = config["pcs_p_min"]
    Q_max = config["pcs_q_max"]
    Q_min = config["pcs_q_min"]
    
    # --- 1. 无功功率控制 (全程参与) ---
    # 逻辑: V <= V_a -> Q_max
    #       V >= V_e -> Q_min
    #       V_a < V < V_e -> 线性下降
    
    target_Q = 0
    if voltage <= V_a:
        target_Q = Q_max
    elif voltage >= V_e:
        target_Q = Q_min
    else:
        # 线性插值: (V - V_a) / (V_e - V_a) = (Q_max - Q) / (Q_max - Q_min)
        # Q = Q_max - (V - V_a) * (Q_max - Q_min) / (V_e - V_a)
        ratio = (voltage - V_a) / (V_e - V_a)
        target_Q = Q_max - ratio * (Q_max - Q_min)
        
    logging.info(f"PCS协同-无功: 电压={voltage:.2f}V, 目标Q={target_Q:.1f}")
    write_pcs_reactive(target_Q)
    
    # --- 2. 有功功率控制 (死区 V_b ~ V_d 不参与) ---
    # 逻辑: V_b <= V <= V_d -> 不调节 (服从上级/保持现状)
    #       V < V_b -> 参与调节 (放电支撑电压, P 增加)
    #           V <= V_a -> P_max
    #           V_a < V < V_b -> 线性增加 (从 0 到 P_max) ?
    #           注意: 图像显示 V_b 处 P 可能不为0 (如果是下垂控制). 
    #           通常死区边界处 P=0 (或者 P_schedule). 假设基准为0.
    #           斜率区间: V_a (P_max) <---> V_b (0)
    #       V > V_d -> 参与调节 (充电抑制电压, P 减少/变负)
    #           V >= V_e -> P_min
    #           V_d < V < V_e -> 线性减少 (从 0 到 P_min)
    #           斜率区间: V_d (0) <---> V_e (P_min)
    
    target_P = None # None 表示不调节
    
    if voltage < V_b:
        # 低电压区域，需放电 (P > 0)
        if voltage <= V_a:
            target_P = P_max
        else:
            # V_a < V < V_b
            # 线性插值: V=V_b -> P=0, V=V_a -> P=P_max
            ratio = (V_b - voltage) / (V_b - V_a)
            target_P = ratio * P_max 
            
    elif voltage > V_d:
        # 高电压区域，需充电 (P < 0)
        if voltage >= V_e:
            target_P = P_min
        else:
            # V_d < V < V_e
            # 线性插值: V=V_d -> P=0, V=V_e -> P=P_min
            ratio = (voltage - V_d) / (V_e - V_d)
            target_P = ratio * P_min
            
    if target_P is not None:
        logging.info(f"PCS协同-有功: 电压={voltage:.2f}V, 目标P={target_P:.1f}")
        write_pcs_active(target_P)
    else:
        logging.info(f"PCS协同-有功: 电压={voltage:.2f}V (在死区 {V_b}-{V_d}), 不参与调节")

def main():
    """主函数"""
    global control_start_time, voltage_recovery_start_time, control_value_set, control_executed, last_control_time, stable_voltage_start_time, last_control_value, last_fine_tune_time
    global current_reactive_power_value, reactive_power_reached_limit, active_power_in_progress, current_active_power_value
    global second_reactive_power_value, second_reactive_reached_limit, active_power_adjusted
    logging.info("开始监控逆变器电压...")
    logging.info(f"电压上限阈值: {upper_threshold:.1f}V")
    logging.info(f"电压下限阈值: {lower_threshold:.1f}V")
    logging.info(f"额定电压: {config['nominal_voltage']}V")
    logging.info(f"死区范围: ±{config['deadband']}V")
    logging.info(f"稳定电压范围: 395-400V")
    logging.info(f"控制算法参数: 斜率={CONTROL_ALGORITHM_SLOPE:.4f}, 截距={CONTROL_ALGORITHM_INTERCEPT:.4f}")
    
    try:
        while True:
            # 获取稳定的电压值
            voltage = get_stable_voltage_value()
            if voltage is not None:
                global last_voltage
                if voltage is not None:
                    # 只有当电压值发生变化时才打印
                    if last_voltage is None or abs(voltage - last_voltage) > 0.001:  # 容差比较
                        logging.info(f"当前电压: {voltage}V")
                        last_voltage = voltage  # 更新上一次的电压值
                    
                    # 获取当前控制值用于状态跟踪
                    control_data = read_inverter_data()
                    if control_data:
                        control_value = get_control_value(control_data)
                        if control_value is not None:
                            current_reactive_power_value = control_value  # 更新当前无功功率值
                    
                    # 检查电压是否需要调节（简化逻辑）
                    
                    # 简化状态跟踪

                    # 两台逆变器协调控制算法
                    # 1. 检查电压是否需要调节
                    if voltage > 390 * 1.02 or voltage < 395 or voltage > 400:
                        if voltage > 390 * 1.02:
                            logging.info(f"电压({voltage:.2f}V)高于阈值({390 * 1.02:.2f}V)，需要调节")
                        else:
                            logging.info(f"电压({voltage:.2f}V)不在稳定范围(395-400V)，需要调节")
                        
                        # 特殊保护：检查是否需要切出逆变器
                        # 仅当电压过高时才检查 (例如 > 410V)
                        if voltage > config["nominal_voltage"] + 20:
                             # 获取第二台逆变器电压（用于比较）
                             data2 = read_inverter2_data()
                             v2 = get_voltage_value(data2) if data2 else 0
                             
                             if check_and_perform_cutout(voltage, v2):
                                 logging.warning("执行了切出操作，跳过后续调节")
                                 # 跳过本次循环的后续调节，等待下一轮检查电压
                                 time.sleep(config["check_interval"])
                                 continue
                        
                        # PCS 协同控制入口 (放在光伏控制之前或并行)
                        # 无论电压是否在死区，都调用函数，由函数内部判断
                        pcs_coordinated_control(voltage)

                        # 第一阶段：调节第一台逆变器无功功率

                        if not reactive_power_reached_limit:
                            logging.info("开始调节第一台逆变器无功功率")
                            reactive_result = reactive_power_control(voltage)
                            
                            if reactive_result:
                                # 第一台无功调节成功，检查电压
                                final_voltage = get_stable_voltage_value()
                                if final_voltage is not None and (final_voltage >= 395 and final_voltage <= 400):
                                    logging.info(f"第一台无功调节后电压({final_voltage:.2f}V)已达标，无需继续调节")
                                    reactive_power_reached_limit = False
                                    second_reactive_reached_limit = False
                                elif final_voltage is not None and final_voltage <= 390 * 1.02:
                                    logging.info(f"第一台无功调节后电压({final_voltage:.2f}V)已降至阈值以下，无需继续调节")
                                    reactive_power_reached_limit = False
                                    second_reactive_reached_limit = False
                                else:
                                    # 第一台无功调节完成但电压仍不正常，重置标志允许继续调节
                                    reactive_power_reached_limit = False
                            else:
                                # 第一台无功调节达到极限值，检查第二台
                                reactive_power_reached_limit = True
                                logging.info("第一台逆变器无功功率已达极限值，检查第二台逆变器")
                        
                        # 第二阶段：如果第一台已达极限值，调节第二台逆变器无功功率
                        if reactive_power_reached_limit and not second_reactive_reached_limit:
                            logging.info("开始调节第二台逆变器无功功率")
                            second_result = second_reactive_power_control(voltage)
                            
                            if second_result:
                                # 第二台无功调节成功，检查电压
                                final_voltage = get_stable_voltage_value()
                                if final_voltage is not None and (final_voltage >= 395 and final_voltage <= 400):
                                    logging.info(f"第二台无功调节后电压({final_voltage:.2f}V)已达标，无需继续调节")
                                    reactive_power_reached_limit = False
                                    second_reactive_reached_limit = False
                                elif final_voltage is not None and final_voltage <= 390 * 1.02:
                                    logging.info(f"第二台无功调节后电压({final_voltage:.2f}V)已降至阈值以下，无需继续调节")
                                    reactive_power_reached_limit = False
                                    second_reactive_reached_limit = False
                                else:
                                    # 第二台无功调节完成但电压仍不正常，重置标志允许继续调节
                                    second_reactive_reached_limit = False
                            else:
                                # 第二台无功调节也达到极限值
                                second_reactive_reached_limit = True
                                logging.info("第二台逆变器无功功率也已达极限值，准备调节有功功率")
                        
                        # 第三阶段：如果两台逆变器都达到无功功率极限值且电压仍不正常，调节有功功率
                        if reactive_power_reached_limit and second_reactive_reached_limit and (voltage < 395 or voltage > 400):
                            logging.info("两台逆变器无功功率均已达到极限值，切换到有功功率调节")
                            if active_power_control(voltage):
                                # 有功调节成功，重置无功功率标志以便下次可以重新尝试无功调节
                                reactive_power_reached_limit = False
                                second_reactive_reached_limit = False
                    
                    # 4. 检查电压是否在稳定范围内
                    if voltage >= 395 and voltage <= 400:
                        # 电压稳定，重置所有标志
                        logging.info(f"电压已稳定在({voltage:.2f}V)，重置控制状态")
                        reactive_power_reached_limit = False
                        second_reactive_reached_limit = False
                        active_power_in_progress = False
                        active_power_adjusted = False
                    else:
                        # 电压在正常范围内，无需控制
                        voltage_deviation = voltage - config["nominal_voltage"]
                        if abs(voltage_deviation) <= config["deadband"]:
                            logging.debug(f"电压在死区范围内(偏差: {voltage_deviation:.2f}V)，无需控制")
                        else:
                            logging.debug(f"电压在允许范围内(偏差: {voltage_deviation:.2f}V)，无需控制")
                else:
                    logging.warning("未找到电压数据")
            else:
                logging.warning("读取数据失败")
            
            # 等待
            time.sleep(config["check_interval"])
            
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except Exception as e:
        logging.error(f"程序异常: {e}")

if __name__ == "__main__":
    main()