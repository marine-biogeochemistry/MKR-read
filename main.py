from machine import Pin, UART, I2C, ADC
import time
import bluetooth
import machine
import _thread
from ble_simple_peripheral import BLESimplePeripheral
from comm_manager import CommManager
from ina219 import INA219
from wifi_toggle import PicoPiFileServer
from _thread import allocate_lock

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


# === Relay Setup ===
relay = Pin(13, Pin.OUT) # off

# === RS232 Setup ===
# Setup UART and RS232 direction control pin
uart0 = UART(0, baudrate=9600, tx=Pin(0), rx=Pin(1))
uart1 = UART(1, baudrate=9600, tx=Pin(4), rx=Pin(5))


# === LED Setup ===
# Turn on built-in LED to indicate device is running
led = Pin("LED", Pin.OUT)
led.on()

# Wi‑Fi file server control
wifi_server = None
wifi_thread_running = False

def _wifi_server_thread():
    global wifi_server, wifi_thread_running
    try:
        if wifi_server:
            wifi_server.run()
    except Exception as e:
        print(f"Wi‑Fi server thread error: {e}")
    finally:
        wifi_thread_running = False

def send_rs232_command(command, uart):
    """Send RS232 command and read response - no direction control needed"""
    print(f"Command Sent on UART: {command}")
    
    # RS232 is full-duplex, so just write directly
    uart.write(command + '\r')
    time.sleep(0.05)  # Brief delay for device to process
    
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
    
    return response

# --- File Transfer State ---
receiving_file = False
file_lines = []
schedule = []
startNow = False  # Manual start flag
emergency_stop = False  # Emergency stop flag
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

# Global variables for custom command accumulation
custom_cmd_parts = []
custom_cmd_total_parts = 0

# Global variables for tracking schedule entry parts
current_cmd = None
current_date = None
current_time = None

def on_ble_rx(data):
    global receiving_file, file_lines, startNow, schedule, partial_line, last_packet_time
    global current_cmd, current_date, current_time
    global custom_cmd_parts, custom_cmd_total_parts
    global wifi_server, wifi_thread_running
    
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
            
            elif msg.strip().upper() == 'EMERGENCY_STOP':
                global emergency_stop
                emergency_stop = True
                sp.send('🛑 Emergency stop activated - schedule halted')
                print('🛑 Emergency stop flag set')
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
                
            elif msg.strip().startswith('T:'):
                # Set RTC time from compact format: T:YYYYMMDDHHMMSS
                time_str = msg.strip()[2:]  # Remove 'T:' prefix
                try:
                    print(f"Received compact time string: '{time_str}'")  # Debug output
                    
                    if len(time_str) != 14:
                        raise ValueError(f"Invalid compact time format: expected 14 digits, got {len(time_str)}")
                    
                    # Parse compact format: YYYYMMDDHHMMSS
                    year = int(time_str[0:4])
                    month = int(time_str[4:6])
                    day = int(time_str[6:8])
                    hour = int(time_str[8:10])
                    minute = int(time_str[10:12])
                    second = int(time_str[12:14])
                    
                    print(f"Parsed: {year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}")
                    
                    # Set the RTC using the manager
                    datetime_tuple = (year, month, day, 0, hour, minute, second)  # weekday=0 (Monday)
                    manager.set_rtc_time(datetime_tuple)
                    
                    sp.send(f'🕒 RTC time set to: {year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}')
                    print(f"RTC time set to: {year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}")
                except ValueError as e:
                    sp.send(f'❌ Failed to parse compact time: {e}')
                    print(f"ValueError parsing compact time: {e}")
                    print(f"Time string was: '{time_str}'")
                except Exception as e:
                    sp.send(f'❌ Failed to set RTC time: {e}')
                    print(f"Error setting RTC time: {e}")
                    print(f"Time string was: '{time_str}'")
                return
                
            elif msg.strip().upper() == 'RELAY:ON':
                try:
                    relay.value(0)  # Turn relay ON (active-low)
                    sp.send('🔌 Relay turned ON')
                    print('Relay manually turned ON')
                except Exception as e:
                    sp.send(f'❌ Failed to turn relay ON: {e}')
                    print(f'Error turning relay ON: {e}')
                return
                
            elif msg.strip().upper() == 'RELAY:OFF':
                try:
                    relay.value(1)  # Turn relay OFF (active-low)
                    sp.send('🔌 Relay turned OFF')
                    print('Relay manually turned OFF')
                except Exception as e:
                    sp.send(f'❌ Failed to turn relay OFF: {e}')
                    print(f'Error turning relay OFF: {e}')
                return
                
            elif msg.strip().upper() == 'RELAY:STATUS':
                try:
                    current_state = relay.value()
                    if current_state == 0:
                        status = "ON"
                    else:
                        status = "OFF"
                    sp.send(f'🔌 Relay Status: {status}')
                    print(f'Relay status: {status}')
                except Exception as e:
                    sp.send(f'❌ Failed to get relay status: {e}')
                    print(f'Error getting relay status: {e}')
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
            
            elif msg == "READ_SEQUENCE":
                time.sleep(0.1)  # Small delay before sending response
                # Parse sequence file to extract rinse and pump cycle counts
                rinse_count = 0
                pump_cycles = 0
                try:
                    with open("default_sequence.txt", "r") as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith('RINSE '):
                                try:
                                    rinse_count = int(line.split()[1])
                                except:
                                    pass
                            elif line.startswith('PUMP_CYCLES ') or line.startswith('PUMP '):
                                try:
                                    pump_cycles = int(line.split()[1])
                                except:
                                    pass
                except OSError:
                    sp.send("❌ Error reading sequence file")
                    return
                
                # Send config in format the webpage expects
                sp.send(f"SEQUENCE_CONFIG: RINSE {rinse_count}, PUMP_CYCLES {pump_cycles}")
                return
            
            elif msg.startswith("UPDATE_SEQUENCE:"):
                # Format: UPDATE_SEQUENCE:rinses,pumps (e.g., UPDATE_SEQUENCE:2,12)
                try:
                    params = msg[16:].strip()  # Remove "UPDATE_SEQUENCE:"
                    rinses, pumps = params.split(',')
                    rinse_count = int(rinses)
                    pump_cycles = int(pumps)
                    
                    # Read existing file
                    lines = []
                    try:
                        with open("default_sequence.txt", "r") as f:
                            lines = f.readlines()
                    except OSError:
                        pass
                    
                    # Update the file
                    with open("default_sequence.txt", "w") as f:
                        for line in lines:
                            stripped = line.strip()
                            if stripped.startswith('RINSE '):
                                f.write(f"RINSE {rinse_count}\n")
                            elif stripped.startswith('PUMP_CYCLES ') or stripped.startswith('PUMP '):
                                f.write(f"PUMP_CYCLES {pump_cycles}\n")
                            else:
                                f.write(line)
                    
                    sp.send(f"✅ Sequence updated: {rinse_count} rinses, {pump_cycles} pump cycles")
                except Exception as e:
                    sp.send(f"❌ Error updating sequence: {e}")
                return
            
            elif msg.startswith("wifi_on"):
                # Format: wifi_on [ssid] [password] [port]
                try:
                    
                    parts = msg.split()
                    ssid = parts[1] if len(parts) > 1 else "PICO-AP"
                    password = parts[2] if len(parts) > 2 else "12345678"
                    try:
                        port = int(parts[3]) if len(parts) > 3 else 5001
                    except:
                        port = 5001

                    if wifi_thread_running:
                        sp.send('{"status":"ok","message":"wifi_already_running"}')
                        sp.send('{"message":"Connect your PC to the Wi‑Fi network PICO-AP"}')
                        return

                    wifi_server = PicoPiFileServer(ssid=ssid, password=password, port=port)
                    try:
                        _thread.start_new_thread(_wifi_server_thread, ())
                        wifi_thread_running = True
                        # Give AP a brief moment to come up
                        time.sleep(1)
                        status = wifi_server.status() if hasattr(wifi_server, 'status') else {}
                        ip = status.get('ip') if isinstance(status, dict) else None
                        msg = '{"status":"ok","message":"wifi_started","ip":"' + (ip or '') + '","port":' + str(port) + '}'
                        sp.send(msg)
                        sp.send('{"message":"Connect your PC to the Wi‑Fi network PICO-AP"}')
                    except Exception as e:
                        print(f"Error starting Wi‑Fi server: {e}")
                        sp.send('{"status":"error","message":"wifi_start_failed"}')
                except Exception as e:
                    print(f"wifi_on parse/start error: {e}")
                    sp.send('{"status":"error","message":"wifi_on_error"}')
                return

            elif msg.startswith("wifi_off"):
                try:
                    if wifi_server:
                        try:
                            wifi_server.shutdown()
                        except Exception as e:
                            print(f"Wi‑Fi shutdown error: {e}")
                        wifi_server = None
                        wifi_thread_running = False
                        sp.send('{"status":"ok","message":"wifi_stopped"}')
                    else:
                        sp.send('{"status":"ok","message":"wifi_not_running"}')
                except Exception as e:
                    print(f"wifi_off error: {e}")
                    sp.send('{"status":"error","message":"wifi_off_error"}')
                return

            elif msg.startswith("wifi_status"):
                try:
                    running = bool(wifi_thread_running and wifi_server)
                    status = wifi_server.status() if (wifi_server and hasattr(wifi_server, 'status')) else {}
                    ssid = status.get('ssid', 'PICO-AP') if isinstance(status, dict) else 'PICO-AP'
                    ip = status.get('ip', '') if isinstance(status, dict) else ''
                    port = status.get('port', 5001) if isinstance(status, dict) else 5001
                    ap_active = status.get('ap_active', False) if isinstance(status, dict) else False
                    msg = ('{"status":"ok","running":' + ('true' if running else 'false') +
                        ',"ssid":"' + ssid + '",' +
                        '"ip":"' + (ip or '') + '",' +
                        '"port":' + str(port) + ',' +
                        '"ap_active":' + ('true' if ap_active else 'false') + '}')
                    sp.send(msg)
                except Exception as e:
                    print(f"wifi_status error: {e}")
                    sp.send('{"status":"error","message":"wifi_status_error"}')
                return
            elif msg.startswith("SEND_CMD_START:"):
                parts_str = msg[15:].strip()
                try:
                    custom_cmd_total_parts = int(parts_str)
                    custom_cmd_parts = []
                    sp.send(f"📥 Ready to receive {custom_cmd_total_parts} command parts")
                except ValueError:
                    sp.send("❌ Invalid command start")
                return
                
            elif msg.startswith("SEND_CMD_PART"):
                if custom_cmd_total_parts == 0:
                    sp.send("❌ No command start received")
                    return
                part_num_str = msg.split(":")[0][13:]  # Extract part number
                part_data = msg.split(":", 1)[1] if ":" in msg else ""
                try:
                    part_num = int(part_num_str)
                    custom_cmd_parts.append((part_num, part_data))
                    sp.send(f"📥 Received part {part_num}/{custom_cmd_total_parts}")
                    
                    # Check if all parts received
                    if len(custom_cmd_parts) == custom_cmd_total_parts:
                        # Sort by part number and assemble
                        custom_cmd_parts.sort(key=lambda x: x[0])
                        full_cmd = "".join([part[1] for part in custom_cmd_parts])
                        # Determine which UART based on command (pump=uart0, valve=uart1)
                        target_uart = uart0 if full_cmd.startswith('/1') else uart1
                        send_rs232_command(full_cmd, target_uart)
                        sp.send(f"✅ Sent assembled command: {full_cmd}")
                        # Reset
                        custom_cmd_parts = []
                        custom_cmd_total_parts = 0
                except (ValueError, IndexError):
                    sp.send("❌ Invalid command part")
                return
                
            elif msg == "SEND_CMD_EXEC":
                # This is handled after all parts are received
                return
                
            elif msg.startswith("SEND_CMD:"):
                cmd = msg[9:].strip()
                if cmd:
                    # Determine which UART based on command (pump=uart0, valve=uart1)
                    target_uart = uart0 if cmd.startswith('/1') else uart1
                    send_rs232_command(cmd, target_uart)
                    sp.send(f"✅ Sent command: {cmd}")
                else:
                    sp.send("❌ Empty command")
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
    """Load and parse the schedule file, fallback to default_sequence.txt if not found or empty"""
    global schedule
    schedule = []
    
    def parse_file(file_path):
        temp_schedule = []
        try:
            with open(file_path, "r") as file:
                for line in file:
                    if len(temp_schedule) >= MAX_SCHEDULE_ENTRIES:
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
                        temp_schedule.append(entry)
                    except ValueError:
                        continue
            return temp_schedule
        except OSError:
            return None
    
    # Try primary file
    schedule = parse_file(filename)
    if schedule is not None and schedule:
        print(f"📥 Schedule Loaded from {filename}. Entries: {len(schedule)}")
        return schedule
    elif schedule is None:
        print(f"Failed to open {filename}, trying default_sequence.txt")
    else:
        print(f"{filename} is empty, trying default_sequence.txt")
    
    # Try default file
    schedule = parse_file("default_sequence.txt")
    if schedule is not None and schedule:
        print(f"📥 Default Schedule Loaded from default_sequence.txt. Entries: {len(schedule)}")
        return schedule
    elif schedule is None:
        print("Failed to open default_sequence.txt.")
    else:
        print("default_sequence.txt is empty.")
    
    # If both fail, return empty
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
    last_ble_check = time.ticks_ms()
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


# RS232 functionsdef send_rs232_command(command_str, uart):
def read_and_validate_response(rs232, uart):
    print("Raw Hex Response: ", end='')
    response = bytearray()
    start_time = time.ticks_ms()

    while time.ticks_diff(time.ticks_ms(), start_time) < 2000:
        if rs232.any():
            byte_in = rs232.read(1)
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

    while t < duration_ms:
        time.sleep(0.1)
        t += 100
        current_time = time.ticks_ms()

        # Heartbeat every 5 seconds
        if time.ticks_diff(current_time, last_heartbeat) > 5000:
            last_heartbeat = current_time

            current_now = parse_time_str(manager.get_formatted_time())
            next_switch = ensure_tuple(schedule[next_index]["startTime"])
            remaining = get_safe_remaining_millis(current_now, next_switch)
            
            # Calculate remaining entries
            remaining_entries = len(schedule) - next_index
            
            sp.send("⏰ Current: " + format_time(current_now))
            sp.send("⏭️ Next at: " + format_time(next_switch))
            sp.send(f"📊 Remaining: {remaining_entries}/{len(schedule)} samples")
            

            print(f"⏰ Current: {format_time(current_now)}")
            print(f"⏭️ Next: {format_time(next_switch)}")
            print(f"📊 Remaining: {remaining_entries}/{len(schedule)} samples")

def load_sequence(filename="default_sequence.txt"):
    """Load the execution sequence from file, return list of commands/actions"""
    sequence = []
    try:
        with open(filename, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if line == '{COMMAND}':
                    sequence.append('COMMAND')
                else:
                    sequence.append(line)
        if sequence:
            print(f"📄 Sequence loaded from {filename} with {len(sequence)} steps")
            sp.send(f"📄 Sequence loaded: {len(sequence)} steps")
        return sequence
    except OSError:
        print(f"❌ Failed to load sequence from {filename}")
        sp.send(f"❌ Failed to load sequence from {filename}")
        return []

def test_log(msg):
    """Log messages to testing log file"""
    try:
        t = time.localtime()
        ts = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(t[0], t[1], t[2], t[3], t[4], t[5])
    except Exception:
        ts = str(time.ticks_ms())
    try:
        with open("pump_status_log.txt", "a") as f:
            f.write(ts + " | " + str(msg) + "\n")
    except Exception as e:
        print("test_log failed:", e)

def query_pump_status():
    """Query pump status using [Q] command and decode status byte"""
    uart0.write("/1QR\r")
    time.sleep(0.1)
    
    response = bytearray()
    start_time = time.ticks_ms()
    while time.ticks_diff(time.ticks_ms(), start_time) < 1000:
        if uart0.any():
            byte_in = uart0.read(1)
            if byte_in:
                response.append(byte_in[0])
                if byte_in[0] == 0x0A or len(response) >= 64:
                    break
    
    if len(response) >= 4:
        # Status byte is typically the 4th byte (after /1@)
        status_byte = response[3]
        
        # Bit 5: Pump status (1=ready, 0=busy)
        is_ready = (status_byte & 0x20) != 0
        
        # Bits 0-3: Error code
        error_code = status_byte & 0x0F
        
        # Error descriptions
        errors = {
            0: "Error Free",
            1: "Initialization error - check blockages",
            2: "Invalid Command",
            3: "Invalid Operand",
            6: "EEPROM Failure",
            7: "Device Not Initialized",
            8: "Internal failure",
            9: "Piston Overload - reinitialize required",
            11: "Piston movement not allowed",
            12: "Internal fault",
            14: "A/D converter failure"
        }
        
        status = "READY" if is_ready else "BUSY"
        error_desc = errors.get(error_code, f"Unknown error {error_code}")
        
        print(f"Status: {status} | Error Code: {error_code} ({error_desc})")
        print(f"Raw status byte: 0x{status_byte:02X}")
        
        return {
            'ready': is_ready,
            'error_code': error_code,
            'error_desc': error_desc,
            'status_byte': status_byte,
            'raw_response': response
        }
    else:
        print("⚠️ No valid status response received")
        return None

def wait_for_pump_ready(timeout_sec=40, poll_interval=5):
    """Poll pump status until ready or timeout"""
    global emergency_stop
    print(f"Waiting for pump to become ready (timeout: {timeout_sec}s, polling every {poll_interval}s)")
    sp.send(f"⏳ Monitoring pump status...")
    start_time = time.ticks_ms()
    
    while time.ticks_diff(time.ticks_ms(), start_time) < (timeout_sec * 1000):
        # Check for emergency stop
        if emergency_stop:
            print("🛑 Pump monitoring aborted - emergency stop active")
            sp.send("🛑 Pump aborted - emergency stop")
            return False
        
        status = query_pump_status()
        
        if status:
            # Log status check
            test_log(f"Status check: {'READY' if status['ready'] else 'BUSY'} | Error: {status['error_code']}")
            
            # Send periodic time update while waiting
            current_time_str = manager.get_formatted_time()
            sp.send(f"Current Time")
            sp.send(format_time(ensure_tuple(current_time_str)))
            
            # Check for errors
            if status['error_code'] != 0:
                error_msg = f"⚠️ Pump error: {status['error_desc']}"
                print(error_msg)
                sp.send(error_msg)
                return False
            
            # Check if ready
            if status['ready']:
                print("✅ Pump is ready")
                sp.send("✅ Pump ready")
                return True
            else:
                print(f"⏳ Pump still busy... waiting {poll_interval}s")
        else:
            print("⚠️ Failed to get pump status")
        
        time.sleep(poll_interval)
    
    timeout_msg = f"⏰ Timeout waiting for pump"
    print(timeout_msg)
    sp.send(timeout_msg)
    return False


def send_and_validate(cmd, cycle=None):
    # Transmit
    uart0.write(cmd + '\r')
    time.sleep(0.05)
    # Switch to receive and allow device to respond
    time.sleep(0.1)
    # Read and validate the response
    response, matched = read_and_validate_response(uart0)

    # Prepare a human readable raw hex representation
    if response:
        raw_hex = ",".join([hex(b) for b in response])
    else:
        raw_hex = "[]"

    # Determine status
    if not response or len(response) == 0:
        status = "LOST COMMS"
        print("LOST COMMS")
    else:
        status = "OK" if matched else "MISMATCH"

    # Log the cycle result
    cycle_str = f"Cycle {cycle}" if cycle is not None else ""
    log_msg = f"{cycle_str} | Command: {cmd} | Raw: {raw_hex} | Status: {status}"
    test_log(log_msg)
    print("Logged:", log_msg)
    return response, matched

def execute_step(command):
    global relay_manual_control, emergency_stop
    
    # Check for emergency stop
    if emergency_stop:
        print("🛑 Execute step aborted - emergency stop active")
        sp.send("🛑 Step aborted - emergency stop active")
        return
    
    # Send current status update at start of step
    current_time_str = manager.get_formatted_time()
    sp.send("Current Time")
    sp.send(format_time(ensure_tuple(current_time_str)))
    sp.send(f"🚀 Executing: {command}")
    
    # Clear pump status log at start of new sequence
    try:
        with open("pump_status_log.txt", "w") as f:
            f.write("")
        print("🧹 Pump status log cleared for new sequence")
    except Exception as e:
        print(f"Failed to clear pump status log: {e}")
    
    sp.send("🔀⚡Relay on")
    print("Testing Relay ON")
    # relay off for testing
    relay = Pin(13, Pin.OUT, value=0)  # Initialize and turn ON (active-low)
    time.sleep(2)

    print("Valves and pump set to start positions")
    send_rs232_command("/2wR", uart1)
    time.sleep(1)
    
    # Initialize pump and wait for ready
    print("Initializing pump...")
    sp.send("🔧 Initializing pump")
    send_rs232_command("/1ZWR", uart0) # pump initialization
    if not wait_for_pump_ready(timeout_sec=15, poll_interval=2):
        print("⚠️ Pump initialization failed or timed out")
        sp.send("⚠️ Pump init failed")
        relay.value(1)  # Turn off relay
        return
    
    # probably need a rinse section in here with "/201R and 2 cycles of the pump
    print("Rinsing system")
    send_rs232_command("/2O01R", uart1)
    time.sleep(4)

    sequence = load_sequence("default_sequence.txt")
    if not sequence:
        # Default hard-coded sequence
        sequence = [
            "RINSE 2",
            "COMMAND",
            "PUMP 12"
        ]

    for item in sequence:
        if item.startswith('RINSE '):
            n = int(item.split()[1])
            for i in range(n):
                print(f"💉 Rinse {i+1}/{n}")
                sp.send(f"💉 Rinse {i+1}/{n}")
                send_rs232_command("/1J0S15A0A7640M2000J1M2000S14A0M2000J0R", uart0)
                
                # Wait for pump to complete rinse cycle
                if not wait_for_pump_ready(timeout_sec=40, poll_interval=5):
                    print(f"⚠️ Rinse cycle {i+1} failed")
                    sp.send(f"⚠️ Rinse {i+1} failed")
                    relay.value(1)  # Turn off relay
                    return
        elif item == 'COMMAND':
            print(f"🚀 Executing command: {command}")
            log_command(command, manager.get_formatted_time(), "Start")
            send_rs232_command(command, uart1)
            sp.send("🚀 Executing command: " + command)
            sp.send("🛠️Valves set")
            time.sleep(4)
        elif 'PUMP' in item:
            parts = item.split()
            requested_n = int(parts[-1])
            n = min(requested_n, 15)  # Cap at maximum 15 cycles
            if requested_n > 15:
                warning_msg = f"⚠️ Pump cycles capped at 15 (requested {requested_n})"
                print(warning_msg)
                sp.send(warning_msg)
            print(f"💉 Starting pump sequence ({n} repetitions)")
            sp.send(f"💉 Starting pump sequence ({n}x)")
            
            for i in range(n):
                print(f"💉 Pumping cycle {i+1}/{n}")
                sp.send(f"💉 Cycle {i+1}/{n}")
                
                # Check pump status before sending command
                status = query_pump_status()
                if status and status['error_code'] != 0:
                    error_msg = f"⚠️ Pump error before cycle {i+1}: {status['error_desc']}"
                    print(error_msg)
                    sp.send(error_msg)
                    test_log(f"Cycle {i+1} aborted - pump error: {status['error_desc']}")
                    relay.value(1)  # Turn off relay
                    return
                
                # Send pump command
                send_rs232_command("/1J0S15A0A7640M2000J1M2000S14A0M2000J0R", uart0)
                
                # Wait for pump to complete cycle with status monitoring
                if not wait_for_pump_ready(timeout_sec=45, poll_interval=5):
                    error_msg = f"⚠️ Cycle {i+1} failed or timed out"
                    print(error_msg)
                    sp.send(error_msg)
                    test_log(f"Cycle {i+1} - pump timeout or error")
                    relay.value(1)  # Turn off relay
                    return
                
                print(f"✅ Cycle {i+1} completed successfully")
                test_log(f"Cycle {i+1} completed OK")
            
            print("✅ Completed all pump cycles")
            sp.send("✅ Pumping completed")
            log_command(command, manager.get_formatted_time(), "End")

    print("♻️ Resetting valves post-operation")
    sp.send("♻️Reset valves")
    send_rs232_command("/2wR", uart1)
    time.sleep(4) 

    print("Relay OFF")
    sp.send("🔀⚡Relay off")
    relay.value(1)  # Set relay to OFF state (active-low logic: 1 = OFF, 0 = ON)
    time.sleep(1) 

# Load initial schedule
schedule = load_schedule("schedule.txt")

def main_loop():
    global emergency_stop
    for i, entry in enumerate(schedule):
        # Check for emergency stop before each step
        if emergency_stop:
            print("🛑 Main loop stopped - emergency stop active")
            sp.send("🛑 Schedule stopped - emergency stop")
            break
        
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
    global startNow, emergency_stop  # Declare as global
    while True:
        # Reset emergency stop flag at start of new schedule cycle
        emergency_stop = False
        
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

# Load and confirm sequence on startup
sequence_test = load_sequence("default_sequence.txt")
if not sequence_test:
    print("Using default hard-coded sequence")
    sp.send("Using default hard-coded sequence")

try:
    scheduler()
except KeyboardInterrupt:
    cleanup()
print("\n👋 Script stopped by user")
