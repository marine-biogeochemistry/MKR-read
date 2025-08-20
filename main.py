from machine import Pin, UART, I2C, ADC
import time
import bluetooth
import machine
import _thread
from ble_simple_peripheral import BLESimplePeripheral
from comm_manager import CommManager
from ina219 import INA219

# Power management settings
MIN_BLE_VOLTAGE = 3.6  # Minimum voltage for stable BLE operation 

# Create instance
manager = CommManager()
timestamp = manager.get_formatted_time()
print("Current RTC time:", timestamp)

# Initialize I2C bus
i2c = I2C(1, scl=Pin(7), sda=Pin(6), freq=100000)

# Create INA219 instance
ina219 = INA219(i2c_bus=i2c, addr=0x43)

# Global flag to track relay state
relay_manual_control = False
last_alert_time = 0
last_ok_time = 0    
# last_battery_log = 100  # Track last logged battery level (starts at 100%)

# === Relay Setup ===
relay = Pin(13, Pin.OUT) # off

# === RS485 Setup ===
# Setup UART and RS485 direction control pin
uart = UART(0, baudrate=9600, tx=Pin(0), rx=Pin(1))
rs485_dir = Pin(2, Pin.OUT)  # DE/RE pin

def send_rs485_command(command):
    print("Command Sent:", command)

    # Transmit mode
    rs485_dir.value(1)
    time.sleep(0.01)  # Allow line to settle
    uart.write(command + '\r')
    # time.sleep(0.01)  # Allow transmission to complete

    # Receive mode
    rs485_dir.value(0)
    time.sleep(0.05)  # Wait for response

    # Read response
    start_time = time.ticks_ms()
    response = b""
    while time.ticks_diff(time.ticks_ms(), start_time) < 2000:
        if uart.any():
            response += uart.read()
            if b'\n' in response:
                break

    print("Raw Hex Response:", [hex(b) for b in response])
    try:
        print("Parsed Response:", response.decode('utf-8'))
    except UnicodeError:
        print("Received non-UTF8 data")

# Example command
# send_rs485_command("/3O5R")

# def read_pico_ups():
#     global ble_connected
#     try:
#         # Check system voltage first
#         vsys_voltage = read_vsys()
#         if vsys_voltage < MIN_BLE_VOLTAGE and ble_connected:
#             print(f"⚠️  Low system voltage: {vsys_voltage:.2f}V - BLE may be unstable")
#             # Don't attempt BLE operations if voltage is too low
#             if ble_connected:
#                 ble.active(False)
#                 ble_connected = False
#         elif vsys_voltage >= MIN_BLE_VOLTAGE and not ble_connected:
#             # Try to reinitialize BLE if voltage is back to safe level
#             ble_connected = setup_ble()
            
#         bus_voltage = ina219.getBusVoltage_V()
#         shunt_voltage = ina219.getShuntVoltage_mV()
#         current_mA = ina219.getCurrent_mA()
#         psu_voltage = bus_voltage + (shunt_voltage / 1000)

#         # # Battery percentage based on 3.3V (empty) to 4.2V (full)
#         # min_voltage = 3.3
#         # max_voltage = 4.2
#         # battery_percent = (bus_voltage - min_voltage) / (max_voltage - min_voltage) * 100
#         # battery_percent = max(0, min(100, battery_percent))

#         # Charging status
#         if current_mA < -10:  # Negative current means charging
#             charge_status = "Charging"
#         elif current_mA > 10:
#             charge_status = "Discharging"
#         else:
#             charge_status = "Idle"

#         # print("PSU Voltage: {:6.3f} V".format(psu_voltage))
#         # print("Bus Voltage: {:6.3f} V".format(bus_voltage))
#         # print("Shunt Voltage: {:6.3f} mV".format(shunt_voltage))
#         # print("Current:     {:6.3f} A".format(current_mA / 1000))
#         # print("Battery:     {:6.1f}% ({})".format(battery_percent, charge_status))
#         print("")

#         # Optional warning
#         if battery_percent < 20:
#             print("⚠️  WARNING: Battery below 20% - {:.1f}% remaining".format(battery_percent))

#         return battery_percent

#     except OSError as e:
#         print(f"Error reading from INA219: {e}")
#         print("Please check I2C connections and the address (0x43).")
#         return None



def control_relay(battery_percent, threshold=20):
    global relay_manual_control, last_alert_time, last_ok_time
    
    if relay_manual_control:
        return  # Skip battery control if relay is being manually controlled
    
    current_time = time.ticks_ms()
    
    # Only send alert once per minute to avoid spamming
    alert_interval = 60000  # 60 seconds
    
    if battery_percent < threshold:
        relay.value(1)  # Turn relay OFF (active-low)
        message = f"🔋 LOW BATTERY: {battery_percent:.1f}% - Relay FORCED OFF"
        print(message)
        
        # Check if we need to send an alert
        if time.ticks_diff(current_time, last_alert_time) > alert_interval:
            sp.send("⚠️ " + message)
            last_alert_time = current_time
    else:
        # Don't modify relay state, just log battery status
        message = f"✅ Battery OK: {battery_percent:.1f}%"
        print(message)
        
        # Send OK status less frequently (every 5 minutes)
        ok_interval = 300000  # 5 minutes
        if time.ticks_diff(current_time, last_ok_time) > ok_interval:
            sp.send("✅ " + message)
            last_ok_time = current_time


from _thread import allocate_lock

# --- File Transfer State ---
receiving_file = False
file_lines = []
schedule = []
startNow = False  # Manual start flag
ble_lock = allocate_lock()  # Lock for BLE operations

# --- File Operations ---
def read_log_file():
    """Read the log file and return its contents as a list of lines."""
    try:
        with open("log_ME.txt", "r") as f:
            return f.readlines()
    except OSError as e:
        print(f"Failed to read log file: {e}")
        return ["No log entries found or log file not created yet."]

def clear_log_file():
    """Truncate the log file. Return True on success."""
    try:
        with open("log_ME.txt", "w") as f:
            f.write("")
        return True
    except OSError as e:
        print(f"Failed to clear log file: {e}")
        return False

def read_schedule_file():
    try:
        with open("schedule.txt", "r") as f:
            schedule = []
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                entry = parse_schedule_line(line)
                if entry:
                    schedule.append(entry)
                else:
                    print(f"Skipping invalid schedule entry on line {line_num}: {line}")
                    
            # Sort schedule by timestamp
            schedule.sort(key=lambda x: x['time'])
            print(f"Loaded {len(schedule)} valid schedule entries")
            return schedule
    except Exception as e:
        print(f"Error reading schedule file: {e}")
        return []

# --- BLE Receive Callback ---
# Global variables for BLE message handling
receiving_file = False
file_lines = []
partial_line = ""
last_packet_time = 0
ble_lock = _thread.allocate_lock()

# Global variables for tracking schedule entry parts
current_cmd = None
current_date = None
current_time = None

def on_ble_rx(data):
    global receiving_file, file_lines, startNow, schedule, partial_line, last_packet_time
    global current_cmd, current_date, current_time
    
    last_packet_time = time.ticks_ms()  # Update last packet time
    
    try:
        with ble_lock:  # Acquire lock for thread safety
            # First, just print raw data to see if anything is coming through
            print("\n=== RAW BLE PACKET RECEIVED ===")
            print(f"Data type: {type(data)}")
            print(f"Data length: {len(data) if hasattr(data, '__len__') else 'N/A'}")
            print(f"Data repr: {data}")
            
            # Handle potential binary data or partial messages
            try:
                if isinstance(data, bytes):
                    try:
                        msg = data.decode('utf-8').strip()
                        print(f"📥 RAW BLE DATA (decoded): {msg}")
                    except UnicodeError as ue:
                        # Fallback: replace invalid UTF-8 sequences with '?'
                        msg = data.decode('utf-8', 'ignore').strip()
                        print(f"⚠️  BLE Data had invalid UTF-8: {ue}\n  Decoded with ignore: {msg}")
                else:
                    msg = str(data).strip()
                    print(f"📥 BLE DATA (str): {msg}")
                
                print(f"📥 BLE RX: '{msg}' (type: {type(msg).__name__}, len: {len(msg)})")
                
                if msg == 'GETLOG':
                    print("📄 Sending log file contents...")
                    log_lines = read_log_file()
                    # Send log in chunks to avoid BLE MTU limitations
                    for line in log_lines:
                        ble_send(f"[LOG]{line.strip()}")
                    ble_send("LOG_END")
                    return
                elif msg == 'CLEARLOG':
                    print("🧹 Clearing log file...")
                    ok = clear_log_file()
                    if ok:
                        ble_send("[LOG]Log cleared on device")
                        ble_send("LOG_CLEARED")
                    else:
                        ble_send("[LOG]Failed to clear log on device")
                    return
                elif msg == 'CMD:SCHEDULE_FILE':
                    print("📂 Starting to receive schedule file...")
                    receiving_file = True
                    file_lines = []
                    current_cmd = None
                    current_date = None
                    current_time = None
                    ble_send("ACK:SCHEDULE_START")
                    return
                    
                elif msg == 'CMD:END_SCHEDULE':
                    print("🏁 Finished receiving schedule file")
                    receiving_file = False
                    
                    # Save the last entry if complete
                    if current_cmd and current_date and current_time:
                        entry = f"{current_cmd} at {current_date} {current_time}"
                        file_lines.append(entry)
                        print(f"💾 Saved final entry: {entry}")
                    
                    # Send acknowledgment before processing
                    ble_send("ACK:SCHEDULE_COMPLETE")
                    print("📋 Received schedule entries:")
                    for i, line in enumerate(file_lines, 1):
                        print(f"  {i}. {line}")
                    
                    # Process all received lines
                    if file_lines:
                        schedule = []
                        for line in file_lines:
                            entry = parse_schedule_line(line)
                            if entry:
                                schedule.append(entry)
                        
                        schedule.sort(key=lambda x: x['time'])
                        
                        # Save to file
                        try:
                            with open('schedule.txt', 'w') as f:
                                for entry in schedule:
                                    f.write(f"{entry['command']} at {entry['time']}\n")
                            print(f"✅ Saved {len(schedule)} schedule entries to file")
                            ble_send(f"ACK:SCHEDULE_SAVED {len(schedule)}")
                            
                            # Reload schedule into in-memory structure expected by the scheduler
                            try:
                                loaded = load_schedule()
                                print(f"📥 Schedule reloaded. Entries: {len(loaded)}")
                                ble_send(f"ACK:SCHEDULE_RELOADED {len(loaded)}")
                            except Exception as e:
                                print(f"Error reloading schedule: {e}")
                        except Exception as e:
                            print(f"Error saving schedule: {e}")
                    
                    file_lines = []
                    current_cmd = None
                    current_date = None
                    current_time = None
                    return
                
                elif receiving_file and msg.startswith('DATA:'):
                    data_part = msg[5:].strip()  # Remove 'DATA:' prefix
                    print(f"📥 Processing DATA: '{data_part}'")
                    
                    # Check what type of data we're receiving
                    if data_part.startswith('/') and data_part.endswith('R'):
                        print(f"📥 Received CMD: {data_part}")
                        if current_cmd and current_date and current_time:
                            # Save previous complete entry
                            entry = f"{current_cmd} at {current_date} {current_time}"
                            file_lines.append(entry)
                            print(f"💾 Saved complete entry: {entry}")
                        
                        current_cmd = data_part
                        current_date = None
                        current_time = None
                        ble_send(f"ACK:CMD_RECEIVED {data_part}")
                        
                    elif '-' in data_part and len(data_part) == 10:  # Date format YYYY-MM-DD
                        current_date = data_part
                        print(f"📅 Received date: {current_date}")
                        ble_send(f"ACK:DATE_RECEIVED {current_date}")
                    
                    elif ':' in data_part:  # Likely a time HH:MM:SS
                        current_time = data_part
                        print(f"⏰ Received time: {current_time}")
                        ble_send(f"ACK:TIME_RECEIVED {current_time}")
                    
                    # If we have all three parts, add to entries
                    if current_cmd and current_date and current_time:
                        entry = f"{current_cmd} at {current_date} {current_time}"
                        file_lines.append(entry)
                        print("Added entry:", entry)
                        ble_send(f"ACK:ENTRY_BUILT {len(file_lines)}")
                        
                        # Reset for next entry
                        current_cmd = None
                        current_date = None
                        current_time = None

                    # We've fully handled this DATA packet; avoid falling through to legacy parser
                    return
                
            except Exception as e:
                print("Error processing BLE data:", e)
                return
                
            print("BLE RX:", msg)
            
            # Check for manual start command
            if msg.lower() == 'm':
                startNow = True
                sp.send("✅ Manual start triggered!")
                return

            elif msg.strip().upper() == 'RESET':
                # Reboot the Pico on request via BLE
                try:
                    sp.send('🔁 Rebooting device...')
                except Exception as _e:
                    print('BLE ACK send failed before reboot:', _e)
                time.sleep(0.2)  # allow BLE stack to flush
                machine.reset()
                return

            elif msg.strip().upper() == 'SHUTDOWN':
                try:
                    sp.send('🛑 Shutting down (deep sleep)...')
                except Exception as _e:
                    print('BLE ACK send failed before shutdown:', _e)
                time.sleep(0.2)
                try:
                    machine.deepsleep()
                except Exception as e:
                    print('deepsleep() not supported or failed, resetting instead:', e)
                    machine.reset()
                return
                
            # Check for schedule file start
            if msg == 'CMD:SCHEDULE_FILE':
                receiving_file = True
                file_lines = []
                partial_line = b''
                print("Starting to receive schedule file...")
                sp.send("📝 Receiving schedule file...")
                return

                
            # Check for schedule file end
            if msg == 'CMD:END_SCHEDULE':
                if receiving_file:
                    receiving_file = False
                    print("Finished receiving schedule file")
                    
                    # Process all received lines
                    valid_lines = []
                    for line in file_lines:
                        line = line.strip()
                        if not line:
                            continue
                            
                        # Parse and validate the line
                        entry = parse_schedule_line(line)
                        if entry:
                            valid_lines.append(f"{entry['command']} at {entry['time']}")
                    
                    # Save the valid lines
                    try:
                        with open("schedule.txt", "w") as f:
                            f.write("\n".join(valid_lines) + "\n")
                        
                        # Reload the schedule
                        load_schedule()
                        if schedule:
                            sp.send(f"✅ Schedule saved with {len(schedule)} entries")
                            print(f"Schedule saved with {len(schedule)} entries")
                            
                            # Send first 3 entries as preview
                            for i, entry in enumerate(schedule[:3]):
                                sp.send(f"{i+1}. {entry['command']} at {entry['time']}")
                                time.sleep(0.1)
                            
                            if len(schedule) > 3:
                                sp.send(f"... and {len(schedule)-3} more entries")
                        else:
                            sp.send("⚠️ No valid schedule entries to save")
                            
                    except Exception as e:
                        sp.send(f"⚠️ Failed to process schedule: {e}")
                        import sys
                        sys.print_exception(e)
                    finally:
                        file_lines = []
                return
                
            # Handle data lines (both old and new protocol)
            elif receiving_file:
                try:
                    # Extract the data portion if using DATA: prefix
                    if msg.startswith("DATA:"):
                        line = msg[5:]  # Remove "DATA:" prefix
                    else:
                        line = msg  # Old protocol compatibility
                    
                    # Debug output
                    print(f"Processing line: {line}")
                    
                    # If we have a partial line from before, combine it
                    if partial_line:
                        line = partial_line + line
                        partial_line = ""
                    
                    # Split into individual commands (separated by newlines)
                    parts = line.split('\n')
                    
                    # Process each complete line
                    for part in parts[:-1]:  # All but the last part should be complete lines
                        part = part.strip()
                        if part:  # Skip empty lines
                            file_lines.append(part)
                            sp.send(f"📝 Added: {part}")
                    
                    # Handle the last part (might be incomplete)
                    last_part = parts[-1].strip()
                    if last_part:
                        # Check if it's a complete line (contains 'at' with enough characters after)
                        if ' at ' in last_part and len(last_part.split(' at ')) == 2:
                            file_lines.append(last_part)
                            sp.send(f"📝 Added: {last_part}")
                        else:
                            # Save as partial for next time
                            partial_line = last_part
                            print(f"Saving partial line: {partial_line}")
                            
                except Exception as e:
                    sp.send(f"⚠️ Error processing line: {e}")
                    print(f"Error processing line: {e}")
                    partial_line = ""  # Reset to avoid getting stuck
            
            # Handle other commands
            if msg == "BEGINFILE":
                receiving_file = True
                file_lines = []
                time.sleep(0.1)  # Small delay before sending response
                sp.send("📥 Ready to receive schedule file")
                return
                
            elif msg == "READ_SCHEDULE":
                time.sleep(0.1)  # Small delay before sending response
                schedule_lines = read_schedule_file()
                if schedule_lines:
                    sp.send("📅 Current Schedule:")
                    for line in schedule_lines:
                        # Prefix with [FILE] so the webpage renders schedule lines
                        sp.send(f"[FILE]{line}")
                        time.sleep(0.05)  # Small delay between lines
                else:
                    sp.send("📭 No schedule entries found")
                return
            
    except Exception as e:
        print("BLE RX Error:", e)
        sp.send(f"❌ Error: {str(e)}")

# --- File Operations ---
def save_schedule_file(lines):
    try:
        with open("schedule.txt", "w") as f:
            for line in lines:
                if line.strip() and " at " in line:
                    f.write(line + "\n")
        return True
    except Exception as e:
        print("Save error:", e)
        return False

# --- Setup BLE ---
def setup_ble():
    global ble, sp
    try:
        ble = bluetooth.BLE()
        ble.active(True)
        # Set BLE power level to maximum (8dBm)
        try:
            ble.config(power=0x08)  # 0x08 = +8dBm (maximum)
        except Exception as e:
            print(f"BLE power config not supported: {e}")

        # Try to increase MTU if supported
        try:
            ble.config(mtu=200)  # Request larger MTU (up to 200 bytes per packet)
            print("Pico W BLE initialized with larger MTU")
        except Exception as e:
            print(f"Using default MTU: {e}")

        sp = BLESimplePeripheral(ble)
        sp.on_write(on_ble_rx)  # Set up callback for BLE writes
        return True
    except Exception as e:
        print(f"BLE Setup Error: {e}")
        return False

# Initialize BLE once
ble_connected = setup_ble()
if not ble_connected:
    print("⚠️  Initial BLE setup failed, will retry...")

# Initialize relay
relay = Pin(13, Pin.OUT, value=1)  # Initialize relay in OFF state (active-low)
print("🔌 Relay initialized on GPIO 13")

# VSYS voltage monitoring
vbat_adc = ADC(29)
CONVERSION_FACTOR = 3 * 3.3 / 65535  # 3x voltage divider on Pico

def read_vsys():
    return vbat_adc.read_u16() * CONVERSION_FACTOR

def is_ble_voltage_safe():
    return read_vsys() >= MIN_BLE_VOLTAGE

# (BLE is fully initialized in setup_ble())

# --- BLE send helper ---
def ble_send(message):
    """Safe wrapper for sending BLE notifications."""
    try:
        sp.send(message)
    except Exception as e:
        try:
            # Fallback to string in case of bytes
            sp.send(str(message))
        except Exception:
            print(f"BLE send error: {e}")

# # Initial battery check
# battery_level = read_pico_ups()
# if battery_level is not None:
#     control_relay(battery_level)

# === Load Schedule ===
def parse_schedule_line(line):
    """Parse a single schedule line into command and timestamp"""
    try:
        if ' at ' not in line:
            return None
            
        cmd, timestamp = line.split(' at ', 1)
        cmd = cmd.strip()
        timestamp = timestamp.strip()
        
        # Basic validation
        if not cmd.startswith('/') or not cmd.endswith('R'):
            print(f"Invalid command format: {cmd}")
            return None
            
        # Parse timestamp (format: YYYY-MM-DD HH:MM:SS)
        try:
            # Just validate the format, don't parse it yet
            if len(timestamp) < 16:  # At least "YYYY-MM-DD HH:MM"
                raise ValueError("Timestamp too short")
                
            # Check if we have seconds, if not add them
            if len(timestamp) == 16:  # YYYY-MM-DD HH:MM
                timestamp += ":00"  # Add seconds
                
            return {
                'command': cmd,
                'time': timestamp,
                'raw': line
            }
        except Exception as e:
            print(f"Invalid timestamp '{timestamp}': {e}")
            return None
            
    except Exception as e:
        print(f"Error parsing line: {line} - {e}")
        return None
#                     schedule.append((cmd, (y, m, d, h, mi, s)))
#     except Exception as e:
#         print("Error loading schedule:", e)
#     return schedule


MAX_SCHEDULE_ENTRIES = 100
schedule = []

def parse_schedule_line(line):
    """Parse a single schedule line into command and timestamp"""
    try:
        if not line or not isinstance(line, str):
            return None
            
        line = line.strip()
        if not line or ' at ' not in line:
            return None
            
        # Split into command and timestamp parts
        parts = line.split(' at ', 1)
        if len(parts) != 2:
            return None
            
        cmd = parts[0].strip()
        timestamp = parts[1].strip()
        
        # Basic command validation
        if not cmd or not cmd.startswith('/') or not cmd.endswith('R'):
            print(f"Invalid command format: {cmd}")
            return None
        
        # Clean and validate timestamp
        try:
            # Remove any non-numeric characters except - : and space
            clean_timestamp = ''.join(c for c in timestamp if c.isdigit() or c in ' -:')
            
            # Split into date and time parts
            if ' ' in clean_timestamp:
                date_part, time_part = clean_timestamp.split(' ', 1)
            else:
                date_part = clean_timestamp
                time_part = ''
                
            # Ensure we have at least YYYY-MM-DD
            date_parts = date_part.split('-')
            if len(date_parts) < 3:
                raise ValueError("Invalid date format")
                
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            
            # Default time to 00:00:00 if not provided
            hour = 0
            minute = 0
            second = 0
            
            # Parse time if available
            if time_part:
                time_parts = time_part.split(':')
                if len(time_parts) >= 1:
                    hour = int(time_parts[0])
                if len(time_parts) >= 2:
                    minute = int(time_parts[1])
                if len(time_parts) >= 3:
                    second = int(time_parts[2])
            
            # Basic validation
            if not (1 <= month <= 12 and 1 <= day <= 31 and 
                    0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
                raise ValueError("Invalid date/time values")
            
            # Reconstruct timestamp in consistent format
            formatted_time = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
            
            return {
                'command': cmd,
                'time': formatted_time,
                'raw': line
            }
            
        except (ValueError, IndexError) as e:
            print(f"Invalid timestamp '{timestamp}': {e}")
            return None
            
    except Exception as e:
        print(f"Error parsing line '{line}': {e}")
        return None

def load_schedule(filename="schedule.txt"):
    """Load and parse the schedule file"""
    global schedule
    schedule = []
    try:
        with open(filename, "r") as file:
            for line in file:
                if len(schedule) >= MAX_SCHEDULE_ENTRIES:
                    break

                line = line.strip()
                if " at " not in line:
                    continue

                cmd, datetime_str = line.split(" at ", 1)

                try:
                    y, m, d, h, min, s = map(int, datetime_str.split()[0].split('-') + datetime_str.split()[1].split(':'))
                    entry = {
                        "command": cmd,
                        "startTime": (y, m, d, h, min, s)
                    }
                    schedule.append(entry)
                except ValueError:
                    continue

        print(f"📥 Schedule Loaded. Entries: {len(schedule)}")
        return schedule

    except OSError:
        print("Failed to open schedule file.")
        return []


def rebase_schedule_to_now():
    global schedule

    now = manager.get_formatted_time()  # Returns (year, month, day, weekday, hour, minute, second)
    now_tuple = (now[0], now[1], now[2], now[4], now[5], now[6])  # Strip weekday

    # 🧮 Find earliest scheduled time
    earliest = schedule[0]["startTime"]
    for entry in schedule[1:]:
        if entry["startTime"] < earliest:
            earliest = entry["startTime"]

    # 🕓 Calculate offset and rebase
    def datetime_to_seconds(dt):
        y, m, d, h, min, s = dt
        return (((y * 12 + m) * 30 + d) * 24 + h) * 60 * 60 + min * 60 + s

    earliest_sec = datetime_to_seconds(earliest)
    now_sec = datetime_to_seconds(now_tuple)

    for entry in schedule:
        entry_sec = datetime_to_seconds(entry["startTime"])
        offset = entry_sec - earliest_sec
        rebased_sec = now_sec + offset

        # Convert seconds back to datetime (simplified, not calendar-accurate)
        s = rebased_sec % 60
        rebased_sec //= 60
        min = rebased_sec % 60
        rebased_sec //= 60
        h = rebased_sec % 24
        rebased_sec //= 24
        d = rebased_sec % 30 + 1
        rebased_sec //= 30
        m = rebased_sec % 12 + 1
        y = rebased_sec // 12

        entry["startTime"] = (y, m, d, h, min, s)

    print("📅 Schedule rebased to current time:")
    for i, entry in enumerate(schedule):
        ts = entry["startTime"]
        print(f"  Step {i + 1}: {ts[0]:04d}-{ts[1]:02d}-{ts[2]:02d} {ts[3]:02d}:{ts[4]:02d}:{ts[5]:02d}")


# BATTERY_LOG_FILE = "battery_log.txt"  # Will be saved in current directory
# BATTERY_SHUTDOWN_THRESHOLD = 20.0  # Shutdown at 20% battery
# BATTERY_LOG_INTERVAL = 5 * 60 * 1000  # Log every 5 minutes (in milliseconds)
# last_battery_log_time = 0

import machine

# def safe_shutdown(reason):
#     """Safely shut down the Pico"""
#     print(f"\n⚠️  CRITICAL: {reason}")
#     print("🛑 Initiating safe shutdown...")
    
#     try:
#         # Log the shutdown event
#         with open(BATTERY_LOG_FILE, "a") as log_file:
#             log_file.write(f"{manager.get_formatted_time()} | SYSTEM SHUTDOWN: {reason}\n")
        
#         # Turn off the relay if it's on
#         relay.value(1)  # Assuming IN is the 'off' state
        
#         # Send final BLE message if possible
#         if 'sp' in globals():
#             try:
#                 sp.send(f"🔋 CRITICAL: {reason}")
#                 sp.send("🛑 System shutting down")
#                 time.sleep(1)  # Give time for messages to send
#             except:
#                 pass
        
#         print("Safe shutdown complete. Goodbye!")
        
#         # Blink LED to indicate shutdown
#         led = machine.Pin("LED", machine.Pin.OUT)
#         for _ in range(5):
#             led.on()
#             time.sleep(0.2)
#             led.off()
#             time.sleep(0.2)
        
#         # Go into deep sleep (as close to shutdown as we can get)
#         machine.deepsleep()
        
#     except Exception as e:
#         print(f"Error during shutdown: {e}")
#         # If all else fails, just reset
#         machine.reset()

# def check_battery_safety(battery_level):
#     """Check if battery level is safe, shutdown if critical"""
#     if battery_level is not None and battery_level <= BATTERY_SHUTDOWN_THRESHOLD:
#         safe_shutdown(f"Battery critically low: {battery_level:.1f}% (below {BATTERY_SHUTDOWN_THRESHOLD}%)")

def log_command(cmd, timestamp, start_end):
    try:
        # Map commands to sample positions (handles both 'O' and '0' in commands)
        sample_map = {
            # Format with 'O' and '0' for samples 1-9
            "/2O02R": "smp 1", "/202R": "smp 1",
            "/2O03R": "smp 2", "/203R": "smp 2",
            "/2O04R": "smp 3", "/204R": "smp 3",
            "/2O05R": "smp 4", "/205R": "smp 4",
            "/2O06R": "smp 5", "/206R": "smp 5",
            "/2O07R": "smp 6", "/207R": "smp 6",
            "/2O08R": "smp 7", "/208R": "smp 7",
            "/2O09R": "smp 8", "/209R": "smp 8",
            
            # Format with 'O' and '0' for samples 10-16 (note the different pattern)
            "/2O10R": "smp 9",  "/210R": "smp 9",
            "/2O11R": "smp 10", "/211R": "smp 10",
            "/2O12R": "smp 11", "/212R": "smp 11",
            "/2O13R": "smp 12", "/213R": "smp 12",
            "/2O14R": "smp 13", "/214R": "smp 13",
            "/2O15R": "smp 14", "/215R": "smp 14",
            "/2O16R": "smp 15", "/216R": "smp 15"
        }
        
        # Get sample position or default to "N/A"
        sample = sample_map.get(cmd.strip(), "N/A")
        
        # # Read battery status
        # battery_level = read_pico_ups()
        # battery_status = f"{battery_level:.1f}%" if battery_level is not None else "N/A"
        
        # # Log battery status
        # if battery_level is not None:
        #     log_battery_status(battery_level)
        
        # # Check for battery level drop
        # if battery_level is not None:
        #     log_battery_drop(battery_level)
        
        with open("log_ME.txt", "a") as log_file:
            # If timestamp is already a string, use it directly
            if isinstance(timestamp, str):
                timestamp_str = timestamp
            else:
                # Otherwise, format it as YYYY-MM-DD HH:MM:SS
                try:
                    timestamp_str = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
                        int(timestamp[0]), int(timestamp[1]), int(timestamp[2]),
                        int(timestamp[3]), int(timestamp[4]), int(timestamp[5])
                    )
                except (IndexError, ValueError, TypeError):
                    # Fallback to current time if timestamp is invalid
                    now = time.localtime()
                    timestamp_str = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
                        now[0], now[1], now[2], now[3], now[4], now[5]
                    )
            
            log_entry = f"{timestamp_str} | Command: {cmd} | Sample: {sample} | Status: {start_end}\n"
            log_file.write(log_entry)
    except OSError as e:
        print(f"Failed to write to log file: {e}")


def wait_for_start():
    global startNow, schedule, ble_connected
    last_ping = time.ticks_ms()
    # last_battery_check = 0
    last_ble_check = time.ticks_ms()
    # battery_update_interval = 5000  # 5 seconds
    ble_check_interval = 10000  # 10 seconds
    
    # Load schedule if it's empty
    if not schedule:
        print("📋 Loading schedule...")
        schedule = load_schedule("schedule.txt")
        if not schedule:
            print("❌ No schedule found or schedule is empty")
            sp.send("❌ No schedule found")
            return
        print(f"📋 Schedule loaded with {len(schedule)} entries")

    while True:
        current_time = time.ticks_ms()
        
        # # Check battery status every 5 seconds
        # if time.ticks_diff(current_time, last_battery_check) > battery_update_interval:
        #     battery_level = read_pico_ups()
        #     if battery_level is not None:
        #         control_relay(battery_level)  # Update relay state
        #         battery_status = f"Battery: {battery_level:.1f}%"
        #         if ble_connected:
        #             try:
        #                 sp.send(f"🔋 {battery_status}")
        #             except:
        #                 print("⚠️  BLE send failed, will retry...")
        #                 ble_connected = False
        #         # Log battery status
        #         # log_battery_status(battery_level)
        #     else:
        #         battery_status = "Battery: --%"
        #     last_battery_check = current_time
        # else:
        #     battery_status = ""  # Don't show battery status if not updated this cycle
            
        # Periodically check BLE connection
        if time.ticks_diff(current_time, last_ble_check) > ble_check_interval:
            if not ble_connected and read_vsys() >= MIN_BLE_VOLTAGE:
                print("🔄 Attempting to reconnect BLE...")
                ble_connected = setup_ble()
            last_ble_check = current_time

        if time.ticks_diff(current_time, last_ping) > 10000:
            last_ping = current_time
            sp.send("🔄 Waiting for start command or schedule...")
            # sp.send("Press m for manual start")
            print("🔄 Sent BLE ping: Waiting for start...")

        # Get current time as a formatted string and convert to tuple
        current_time_str = manager.get_formatted_time()
        now_tuple = ensure_tuple(current_time_str)
        
        if not schedule:
            print("❌ No schedule entries available")
            sp.send("❌ No schedule entries")
            time.sleep(5)
            continue
            
        scheduled_tuple = ensure_tuple(schedule[0]["startTime"])

        print("-----------")
        # print(f"Current Time: {format_time(now_tuple)} {battery_status}")
        print(f"Scheduled Start: {format_time(scheduled_tuple)}")
        print("BLE Ready - Send 'm' for manual start")

        sp.send("Current Time")
        sp.send(format_time(now_tuple))
        sp.send("Start Time")
        sp.send(format_time(scheduled_tuple))

        print("📋 Scheduled Sequence:")
        for i, entry in enumerate(schedule):
            dt = ensure_tuple(entry["startTime"])
            print(f"  {i + 1}. {format_time(dt)} -> {entry['command']}")

        if len(schedule) >= 2:
            start_dt = ensure_tuple(schedule[0]["startTime"])
            end_dt = ensure_tuple(schedule[-1]["startTime"])
            duration = seconds_between(start_dt, end_dt)
            hrs, rem = divmod(duration, 3600)
            mins, secs = divmod(rem, 60)

            msg_runtime = f"🕒 Scheduled Runtime: {hrs} hrs {mins} min {secs} sec"
            msg_end = f"🛑 Scheduled End Time: {format_time(end_dt)}"

            print(msg_runtime)
            print(msg_end)
            sp.send(msg_runtime)
            sp.send(msg_end)

        now_sec = datetime_to_seconds(now_tuple)
        scheduled_sec = datetime_to_seconds(scheduled_tuple)

        # Check for manual start or scheduled time reached
        if startNow or (scheduled_sec <= now_sec <= scheduled_sec + 10):
            if startNow:
                msg = "✅ Manual start triggered!"
                startNow = False  # Reset for next time
            else:
                msg = "✅ Scheduled start time reached (within 10-second grace period)!"
            print(msg)
            sp.send(msg)
            break
            
        elif now_sec > scheduled_sec + 10:
            # Skip past any schedule entries that have already passed
            while schedule and now_sec > datetime_to_seconds(ensure_tuple(schedule[0]["startTime"])) + 10:
                skipped = schedule.pop(0)
                print(f"⏭️  Skipped past schedule entry at {skipped['startTime']}")
                sp.send(f"⏭️  Skipped: {skipped['startTime']}")
            
            if not schedule:
                print("❌ No more schedule entries available")
                sp.send("❌ No more schedule entries")
                return
                
            # Update the scheduled_tuple for the next iteration
            scheduled_tuple = ensure_tuple(schedule[0]["startTime"])
            print(f"⏭️  Next scheduled time: {format_time(scheduled_tuple)}")
            sp.send(f"Next: {format_time(scheduled_tuple)}")

        time.sleep(5)


# Helper time functions
def parse_time_str(s):
    y, m, d, h, min, sec = map(int, s.replace("-", " ").replace(":", " ").split())
    return (y, m, d, h, min, sec)

def format_time(dt):
    y, m, d, h, min, sec = map(int, dt)
    return f"{y:04d}-{m:02d}-{d:02d} {h:02d}:{min:02d}:{sec:02d}"

def datetime_to_seconds(dt):
    try:
        y, m, d, h, min, s = map(int, dt)
        return h * 3600 + min * 60 + s + d * 86400
    except Exception as e:
        print(f"⚠️ Bad datetime input to seconds: {dt} → {e}")
        return 0  # Or fallback value

def seconds_between(start_dt, end_dt):
    return datetime_to_seconds(end_dt) - datetime_to_seconds(start_dt)


def ensure_tuple(value):
    if isinstance(value, str):
        # Handle "YYYY-MM-DD HH:MM:SS" format
        try:
            date_part, time_part = value.split(' ')
            y, m, d = map(int, date_part.split('-'))
            h, mi, s = map(int, time_part.split(':'))
            return (y, m, d, h, mi, s)
        except (ValueError, AttributeError):
            return (0, 0, 0, 0, 0, 0)
    elif isinstance(value, (list, tuple)) and len(value) >= 6:
        return tuple(value[:6])
    return (0, 0, 0, 0, 0, 0)

def print_current_time(now):
    formatted_now = format_time(now)
    print(f"🕒 Current Time: {formatted_now}")
    sp.send(f"🕒 Current Time: {formatted_now}")  
    
def get_safe_remaining_millis(current, next_time):
    try:
        current_sec = datetime_to_seconds(ensure_tuple(current))
        next_sec = datetime_to_seconds(ensure_tuple(next_time))
        delta_ms = (next_sec - current_sec) * 1000
        return max(delta_ms, 0)
    except Exception as e:
        print(f"⚠️ Timing calc failed: {e}")
        return 0
    
def print_timing_info(step, remaining_ms, next_switch, now):
    # Show current time
    print_current_time(now)

    command = schedule[step]['command']
    step_count = len(schedule)
    time_left_min = remaining_ms // 60000
    formatted_next = format_time(next_switch)

    print(f"Command Sent: {command}")
    print(f"Step {step + 1}/{step_count} | Time Left: {time_left_min} minutes | Next Switch At: {formatted_next}")

    sp.send(f"🧭 Step {step + 1}/{step_count}")
    sp.send(f"➡ Command: {command}")
    sp.send(f"⏱ Time Left: {time_left_min} minutes")
    sp.send(f"📍 Next Switch At: {formatted_next}")


# RS485 functions
def read_and_validate_response(rs485):
    print("Raw Hex Response: ", end='')
    response = bytearray()
    start_time = time.ticks_ms()

    while time.ticks_diff(time.ticks_ms(), start_time) < 2000:
        if rs485.any():
            byte_in = rs485.read(1)
            if byte_in:
                print(f"{byte_in[0]:02X} ", end='')
                response.append(byte_in[0])
                if byte_in[0] == 0x0A or len(response) >= 64:
                    break
    print()

    print("Parsed Response (Hex): ", end='')
    for b in response:
        print(f"{b:02X} ", end='')
    print()

    expected_frame = bytearray([0xFF, 0x2F, 0x30, 0x40, 0x03, 0x0D, 0x0A])
    matched = response == expected_frame

    if matched:
        print("✅ Response matches expected frame.")
    else:
        print("⚠️ Response does NOT match expected frame!")
        print("Expected: ", end='')
        for b in expected_frame:
            print(f"{b:02X} ", end='')
        print()
        print("Received: ", end='')
        for b in response:
            print(f"{b:02X} ", end='')
        print()


def wait_with_heartbeat(duration_ms, next_index):
    t = 0
    last_heartbeat = time.ticks_ms()
    # last_battery_check = 0
    # battery_update_interval = 30000  # Check battery every 30 seconds
    # battery_level = None

    while t < duration_ms:
        time.sleep(0.1)
        t += 100
        current_time = time.ticks_ms()

        # # Check if it's time for a battery update (every 30 seconds)
        # if time.ticks_diff(current_time, last_battery_check) > battery_update_interval:
        #     battery_level = read_pico_ups()
        #     if battery_level is not None:
        #         control_relay(battery_level)  # Update relay state based on battery
        #     last_battery_check = current_time

        # Heartbeat every 5 seconds
        if time.ticks_diff(current_time, last_heartbeat) > 5000:
            last_heartbeat = current_time

            current_now = parse_time_str(manager.get_formatted_time())
            next_switch = ensure_tuple(schedule[next_index]["startTime"])
            remaining = get_safe_remaining_millis(current_now, next_switch)

            # Send all heartbeat information
            # sp.send("💓Heartbeat active")
            # sp.send(f"🔋 Battery: {battery_level:.1f}%" if battery_level is not None else "🔋 Battery: --%")
            sp.send("⏰ Current: " + format_time(current_now))
            sp.send("⏭️ Next at: " + format_time(next_switch))
            
            # Debug prints
            # print(f"💓 BLE heartbeat - Battery: {battery_level:.1f}%")
            print(f"⏰ Current: {format_time(current_now)}")
            print(f"⏭️ Next: {format_time(next_switch)}")

def execute_step(command):
    global relay_manual_control
    # relay_manual_control = True  # Disable battery control of relay
    
    sp.send("🔀⚡Relay on")
    print("Testing Relay ON")
    # relay off for testing
    relay = Pin(13, Pin.OUT, value=0)  # Initialize and turn ON (active-low)
    time.sleep(2)

    print("Valves and pump set to start positions")
    send_rs485_command("/2wR")
    time.sleep(1)
    send_rs485_command("/1ZWR") # pump test
    time.sleep(10)

    # probably need a rinse section in here with "/201R and 2 cycles of the pump
    print( "Rinsing system")
    send_rs485_command("/2O01R")
    time.sleep(4)
    for i in range(2):
        print(f"💉 Rinse {i+1}/2")
        sp.send(f"💉 Rinse {i+1}/2")
        send_rs485_command("/1J0S14A0A7640J1M500S14A0M500J0R")
        time.sleep(23)  # 23 secomds at least for each cycle. 


    print(f"🚀 Executing command: {command}")
    log_command(command, manager.get_formatted_time(), "Start")
    send_rs485_command(command)
    sp.send("🚀 Executing command: " + command)
    sp.send("🛠️Valves set")
    time.sleep(4)

    print("💉 Starting pump sequence (15 repetitions)")
    sp.send("💉 Starting pump sequence (15x)")
    
    for i in range(15): # normanly 15
        print(f"💉 Pumping cycle {i+1}/15")
        sp.send(f"💉 Cycle {i+1}/15")
        send_rs485_command("/1J0S14A0A7640J1M500S14A0M500J0R")
        time.sleep(23)  # 23 secomds at least for each cycle. 

    print("✅ Completed all pump cycles")
    sp.send("✅ Pumping completed")

    log_command(command, manager.get_formatted_time(), "End")
    
    print("♻️ Resetting valves post-operation")
    sp.send("♻️Reset valves")
    send_rs485_command("/2wR")
    time.sleep(4)
    
    # # Log battery status after sample collection
    # battery_level = read_pico_ups()
    # if battery_level is not None:
    #     log_battery_status(battery_level, force_log=True)
    

    print("Relay OFF")
    sp.send("🔀⚡Relay off")
    relay.value(1)  # Set relay to OFF state (active-low logic: 1 = OFF, 0 = ON)
    time.sleep(1)  # Small delay before re-enabling battery control
    # relay_manual_control = False  # Re-enable battery control

schedule = load_schedule("schedule.txt")

def main_loop():
    for i, entry in enumerate(schedule):
        print("--------------------------------------------------")
        now_tuple = parse_time_str(manager.get_formatted_time())
        step_start = time.ticks_ms()

        execute_step(entry['command'])

        step_duration = time.ticks_diff(time.ticks_ms(), step_start)
        next_switch = ensure_tuple(schedule[i + 1]["startTime"]) if i < len(schedule) - 1 else now_tuple
        remaining = get_safe_remaining_millis(manager.get_formatted_time(), next_switch)
        percent = ((i + 1) * 100) // len(schedule)

        # 🎯 Step Summary & Progress
        sp.send(f"📊 Progress: {percent}%")
        sp.send(f"⏱️ Step {i + 1} Duration: {step_duration // 1000} sec")
        sp.send(f"✅ Step {i + 1} executed: {entry['command']}")
        print_timing_info(i, remaining, next_switch, now_tuple)

        # ⏳ Delay to next scheduled step
        if i < len(schedule) - 1:
            wait_with_heartbeat(remaining, i + 1)

    print("🎉 All scheduled steps completed!")
    sp.send("All steps completed!")
    
def scheduler():
    global startNow  # Declare startNow as global
    while True:
        wait_for_start()  # Wait for manual BLE trigger or scheduled start
        main_loop()       # Execute scheduled steps
        
        # After completing the schedule, wait for new schedule or manual start
        print("\n📅 Schedule completed. Waiting for new schedule or manual start...")
        sp.send("📅 Schedule completed. Waiting for new schedule")
        
        # Check for new schedule every 5 seconds
        while True:
            # Check for manual start
            if startNow:
                print("\nManual start detected!")
                startNow = False
                break
                
            # Check if schedule file has been updated
            try:
                new_schedule = load_schedule("schedule.txt")
                if new_schedule:
                    print("\nNew schedule detected!")
                    schedule.clear()
                    schedule.extend(new_schedule)
                    break
            except Exception as e:
                print(f"\nError checking schedule: {e}")
            
            time.sleep(5)  # Check every 5 seconds
        time.sleep(5)

def cleanup():
    print("\n🛑 Cleaning up before exit...")
    # Turn off the relay
    relay.value(1)  # Assuming 1 is the 'off' state based on your relay test
    # Close any open connections
    if 'sp' in globals():
        sp.send("Script stopped by user")
    print("Cleanup complete. Safe to disconnect.")

try:
    scheduler()
except KeyboardInterrupt:
    cleanup()
print("\n👋 Script stopped by user")
