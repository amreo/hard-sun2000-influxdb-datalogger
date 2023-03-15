use chrono::{Local, LocalResult, NaiveDateTime, TimeZone};
use influxdb::{Client, InfluxDbWriteable, Timestamp, Type};
use io::ErrorKind;
use simplelog::*;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use tokio_modbus::client::Context;
use tokio_modbus::prelude::*;

pub const SUN2000_POLL_INTERVAL_SECS: f32 = 2.0; //secs between polling
pub const SUN2000_STATS_DUMP_INTERVAL_SECS: f32 = 3600.0; //secs between showing stats
pub const SUN2000_ATTEMPTS_PER_PARAM: u8 = 3; //max read attempts per single parameter

// Just a generic Result type to ease error handling for us. Errors in multithreaded
// async contexts needs some extra restrictions
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
pub enum ParamKind {
    Text(Option<String>),
    NumberU16(Option<u16>),
    NumberI16(Option<i16>),
    NumberU32(Option<u32>),
    NumberI32(Option<i32>),
}

impl fmt::Display for ParamKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParamKind::Text(v) => write!(f, "Text: {}", v.clone().unwrap()),
            ParamKind::NumberU16(v) => write!(f, "NumberU16: {}", v.unwrap()),
            ParamKind::NumberI16(v) => write!(f, "NumberI16: {}", v.unwrap()),
            ParamKind::NumberU32(v) => write!(f, "NumberU32: {}", v.unwrap()),
            ParamKind::NumberI32(v) => write!(f, "NumberI32: {}", v.unwrap()),
        }
    }
}

pub struct ParameterBlock {
    reg_address: u16,
    len: u16,
    parameters: Vec<Parameter>
}

impl ParameterBlock {
    pub fn new(
        parameters: Vec<Parameter>
    ) -> Self {
        //parameters pre-condition that is memory contigue
        let mut end_reg_address = parameters[0].reg_address;
        parameters.iter().for_each(|p| {
            
            if p.reg_address != end_reg_address {
                panic!("The code is broken in ParameterBlock::new. Invalid start address for {:?} {:?}. The expected start address was {:?}", p.name, p.reg_address, end_reg_address);
            } else {
                end_reg_address = p.reg_address + p.len;
            }
        });

        Self {
            reg_address: parameters[0].reg_address,
            len: end_reg_address - parameters[0].reg_address,
            parameters
        }
    }
   
}

#[derive(Clone)]
pub struct Parameter {
    name: String,
    value: ParamKind,
    desc: Option<&'static str>,
    unit: Option<&'static str>,
    gain: u16,
    reg_address: u16,
    len: u16,
    initial_read: bool,
    save_to_influx: bool,
}

impl Parameter {
    pub fn new(
        name: &'static str,
        value: ParamKind,
        desc: Option<&'static str>,
        unit: Option<&'static str>,
        gain: u16,
        reg_address: u16,
        len: u16,
        initial_read: bool,
        save_to_influx: bool,
    ) -> Self {
        Self {
            name: String::from(name),
            value,
            desc,
            unit,
            gain,
            reg_address,
            len,
            initial_read,
            save_to_influx,
        }
    }

    pub fn new_from_string(
        name: String,
        value: ParamKind,
        desc: Option<&'static str>,
        unit: Option<&'static str>,
        gain: u16,
        reg_address: u16,
        len: u16,
        initial_read: bool,
        save_to_influx: bool,
    ) -> Self {
        Self {
            name,
            value,
            desc,
            unit,
            gain,
            reg_address,
            len,
            initial_read,
            save_to_influx,
        }
    }

    pub fn get_text_value(&self) -> String {
        match &self.value {
            ParamKind::Text(v) => v.clone().unwrap(),
            ParamKind::NumberU16(v) => {
                if self.gain != 1 {
                    (v.unwrap() as f32 / self.gain as f32).to_string()
                } else {
                    v.unwrap().to_string()
                }
            }
            ParamKind::NumberI16(v) => {
                if self.gain != 1 {
                    (v.unwrap() as f32 / self.gain as f32).to_string()
                } else {
                    v.unwrap().to_string()
                }
            }
            ParamKind::NumberU32(v) => {
                if self.gain != 1 {
                    (v.unwrap() as f32 / self.gain as f32).to_string()
                } else if self.unit.unwrap_or_default() == "epoch" {
                    match *v {
                        Some(epoch_secs) => {
                            let naive = NaiveDateTime::from_timestamp_opt(epoch_secs as i64, 0);
                            match Local.from_local_datetime(&naive.unwrap()) {
                                LocalResult::Single(dt) => {
                                    format!("{}, {:?}", epoch_secs, dt.to_rfc2822())
                                }
                                _ => "timestamp conversion error".into(),
                            }
                        }
                        None => "None".into(),
                    }
                } else {
                    v.unwrap().to_string()
                }
            }
            ParamKind::NumberI32(v) => {
                if self.gain != 1 {
                    (v.unwrap() as f32 / self.gain as f32).to_string()
                } else {
                    v.unwrap().to_string()
                }
            }
        }
    }

    pub fn get_influx_value(&self) -> influxdb::Type {
        match &self.value {
            ParamKind::Text(v) => {
                Type::Text(v.clone().unwrap())
            }
            ParamKind::NumberU16(v) => {
                if self.gain != 1 {
                    Type::Float(v.unwrap() as f64 / self.gain as f64)
                } else {
                    Type::SignedInteger(v.unwrap() as i64)
                }
            }
            ParamKind::NumberI16(v) => {
                if self.gain != 1 {
                    Type::Float(v.unwrap() as f64 / self.gain as f64)
                } else {
                    Type::SignedInteger(v.unwrap() as i64)
                }
            }
            ParamKind::NumberU32(v) => {
                if self.gain != 1 {
                    Type::Float(v.unwrap() as f64 / self.gain as f64)
                } else {
                    Type::SignedInteger(v.unwrap() as i64)
                }
            }
            ParamKind::NumberI32(v) => {
                if self.gain != 1 {
                    Type::Float(v.unwrap() as f64 / self.gain as f64)
                } else {
                    Type::SignedInteger(v.unwrap() as i64)
                }
            }
        }
    }
}

fn get_attribute_name(id: &str) -> &'static str {
    let device_description_attributes = vec![
        (1, "Device model"),
        (2, "Device software version"),
        (3, "Port protocol version"),
        (4, "ESN"),
        (5, "Device ID"),
        (6, "Feature version"),
    ];
    if let Ok(id) = id.parse::<u8>() {
        for elem in device_description_attributes {
            if elem.0 == id {
                return elem.1;
            }
        }
    }
    "Unknown attribute"
}

pub struct Sun2000 {
    pub name: String,
    pub host_port: String,
    pub poll_ok: u64,
    pub poll_errors: u64,
    pub influxdb_url: Option<String>,
    pub influxdb_token: Option<String>,
    pub mode_change_script: Option<String>,
    pub dongle_connection: bool,
    pub partial: bool,
}

impl Sun2000 {
    #[rustfmt::skip]
    pub fn param_table() -> Vec<ParameterBlock> {
        vec![
            ParameterBlock::new(vec![
                Parameter::new("model_name", ParamKind::Text(None), None,  None, 1, 30000, 15, true, true),
                Parameter::new("serial_number", ParamKind::Text(None), None,  None, 1, 30015, 10, true, true),
                Parameter::new("product_number", ParamKind::Text(None), None,  None, 1, 30025, 10, true, true),
            ]),
            ParameterBlock::new(vec![
                Parameter::new("model_id", ParamKind::NumberU16(None), None, None, 1, 30070, 1, true, true),
                Parameter::new("nb_pv_strings", ParamKind::NumberU16(None), None, None, 1, 30071, 1, true, true),
                Parameter::new("nb_mpp_tracks", ParamKind::NumberU16(None), None, None, 1, 30072, 1, true, true),
                Parameter::new("rated_power", ParamKind::NumberU32(None), None, Some("W"), 1, 30073, 2, true, true),
                Parameter::new("P_max", ParamKind::NumberU32(None), None, Some("W"), 1, 30075, 2, false, true),
                Parameter::new("S_max", ParamKind::NumberU32(None), None, Some("VA"), 1, 30077, 2, false, true),
                Parameter::new("Q_max_out", ParamKind::NumberI32(None), None, Some("VAr"), 1, 30079, 2, false,true),
                Parameter::new("Q_max_in", ParamKind::NumberI32(None), None, Some("VAr"), 1, 30081, 2, false, true),
            ]),
            ParameterBlock::new(vec![
                Parameter::new("state_1", ParamKind::NumberU16(None), None, Some("state_bitfield16"), 1, 32000, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("state_2", ParamKind::NumberU16(None), None, Some("state_opt_bitfield16"), 1, 32002, 1, false, true),
                Parameter::new("state_3", ParamKind::NumberU32(None), None, Some("state_opt_bitfield32"), 1, 32003, 2, false, true),
            ]),
            ParameterBlock::new(vec![    
                Parameter::new("alarm_1", ParamKind::NumberU16(None), None, Some("alarm_bitfield16"), 1, 32008, 1, false, true),
                Parameter::new("alarm_2", ParamKind::NumberU16(None), None, Some("alarm_bitfield16"), 1, 32009, 1, false, true),
                Parameter::new("alarm_3", ParamKind::NumberU16(None), None, Some("alarm_bitfield16"), 1, 32010, 1, false, true),
            ]),
            ParameterBlock::new(vec![    
                Parameter::new_from_string(format!("pv_{:02}_voltage", 1), ParamKind::NumberI16(None), None, Some("V"), 10, 32014 + 2, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_current", 1), ParamKind::NumberI16(None), None, Some("A"), 100, 32015 + 2, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_voltage", 2), ParamKind::NumberI16(None), None, Some("V"), 10, 32014 + 4, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_current", 2), ParamKind::NumberI16(None), None, Some("A"), 100, 32015 + 4, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_voltage", 3), ParamKind::NumberI16(None), None, Some("V"), 10, 32014 + 6, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_current", 3), ParamKind::NumberI16(None), None, Some("A"), 100, 32015 + 6, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_voltage", 4), ParamKind::NumberI16(None), None, Some("V"), 10, 32014 + 8, 1, false, true),
                Parameter::new_from_string(format!("pv_{:02}_current", 4), ParamKind::NumberI16(None), None, Some("A"), 100, 32015 + 8, 1, false, true),
            ]),
            ParameterBlock::new(vec![       
                Parameter::new("input_power", ParamKind::NumberI32(None), None, Some("W"), 1, 32064, 2, false, true),
                Parameter::new("line_voltage_A_B", ParamKind::NumberU16(None), Some("grid_voltage"), Some("V"), 10, 32066, 1, false, true),
                Parameter::new("line_voltage_B_C", ParamKind::NumberU16(None), None, Some("V"), 10, 32067, 1, false, true),
                Parameter::new("line_voltage_C_A", ParamKind::NumberU16(None), None, Some("V"), 10, 32068, 1, false, true),
                Parameter::new("phase_A_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 32069, 1, false, true),
                Parameter::new("phase_B_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 32070, 1, false, true),
                Parameter::new("phase_C_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 32071, 1, false, true),
                Parameter::new("phase_A_current", ParamKind::NumberI32(None), Some("grid_current"), Some("A"), 1000, 32072, 2, false, true),
                Parameter::new("phase_B_current", ParamKind::NumberI32(None), None, Some("A"), 1000, 32074, 2, false, true),
                Parameter::new("phase_C_current", ParamKind::NumberI32(None), None, Some("A"), 1000, 32076, 2, false, true),
                Parameter::new("day_active_power_peak", ParamKind::NumberI32(None), None, Some("W"), 1, 32078, 2, false, true),
                Parameter::new("active_power", ParamKind::NumberI32(None), None, Some("W"), 1, 32080, 2, false, true),
                Parameter::new("reactive_power", ParamKind::NumberI32(None), None, Some("VA"), 1, 32082, 2, false, true),
                Parameter::new("power_factor", ParamKind::NumberI16(None), None, None, 1000, 32084, 1, false, true),
                Parameter::new("grid_frequency", ParamKind::NumberU16(None), None, Some("Hz"), 100, 32085, 1, false, true),
                Parameter::new("efficiency", ParamKind::NumberU16(None), None, Some("%"), 100, 32086, 1, false, true),
                Parameter::new("internal_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 32087, 1, false, true),
                Parameter::new("insulation_resistance", ParamKind::NumberU16(None), None, Some("MΩ"), 100, 32088, 1, false, true),
                Parameter::new("device_status", ParamKind::NumberU16(None), None, Some("status_enum"), 1, 32089, 1, false, true),
                Parameter::new("fault_code", ParamKind::NumberU16(None), None, None, 1, 32090, 1, false, true),
                Parameter::new("startup_time", ParamKind::NumberU32(None), None, Some("epoch"), 1, 32091, 2, false, true),
                Parameter::new("shutdown_time", ParamKind::NumberU32(None), None, Some("epoch"), 1, 32093, 2, false, true),
            ]),
            ParameterBlock::new(vec![
                Parameter::new("accumulated_yield_energy", ParamKind::NumberU32(None), None, Some("kWh"), 100, 32106, 2, false, true),
            ]),
            ParameterBlock::new(vec![
                    Parameter::new("unknown_time_1", ParamKind::NumberU32(None), None, Some("epoch"), 1, 32110, 2, false, true),
            ]),
            ParameterBlock::new(vec![
                Parameter::new("daily_yield_energy", ParamKind::NumberU32(None), None, Some("kWh"), 100, 32114, 2, true,true),
            ]),
            ParameterBlock::new(vec![
                Parameter::new("storage1_status", ParamKind::NumberI16(None), None, Some("storage_status_enum"), 1, 37000, 1, false, true),
                Parameter::new("storage1_charge_discharge_power", ParamKind::NumberI32(None), None, Some("W"), 1, 37001, 2, false, true),
                Parameter::new("storage1_bus_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 37003, 1, false, true),
                Parameter::new("storage1_battery_soc", ParamKind::NumberU16(None), None, Some("%"), 10, 37004, 1, false, true),
            ]),
            ParameterBlock::new(vec![
                Parameter::new("storage_working_mode", ParamKind::NumberU16(None), None, Some("storage_working_mode_enum"), 1, 37006, 1, false, true),
                Parameter::new("storage1_rated_charge_power", ParamKind::NumberU32(None), None, Some("W"), 1, 37007, 2, false, true),
                Parameter::new("storage1_rated_discharge_power", ParamKind::NumberU32(None), None, Some("W"), 1, 37009, 2, false, true),
            ]),
            ParameterBlock::new(vec![    
                Parameter::new("storage1_fault_id", ParamKind::NumberU16(None), None, None, 1, 37014, 1, false, true),
                Parameter::new("storage_current_day_charge_capacity", ParamKind::NumberU32(None), None, Some("kWh"), 100, 37015, 2, false, true),
                Parameter::new("storage_current_day_discharge_capacity", ParamKind::NumberU32(None), None, Some("kWh"), 100, 37017, 2, false, true),
            ]),
            ParameterBlock::new(vec![        
                Parameter::new("storage1_bus_current", ParamKind::NumberI16(None), None, Some("A"), 10, 37021, 1, false, true),
                Parameter::new("storage1_internal_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 37022, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("storage1_remaining_charge_discharge_time", ParamKind::NumberU16(None), None, Some("min"), 1, 37025, 1, false, true),
                Parameter::new("storage1_dcdc_version", ParamKind::Text(None), None, None, 1, 37026, 10, false, true),
                Parameter::new("storage1_bms_version", ParamKind::Text(None), None, None, 1, 37036, 10, false, true),
                Parameter::new("storage1_maximum_charge_power", ParamKind::NumberU32(None), None, Some("W"), 1, 37046, 2, false, true),
                Parameter::new("storage1_maximum_discharge_power", ParamKind::NumberU32(None), None, Some("W"), 1, 37048, 2, false, true),
            ]),
            ParameterBlock::new(vec![                
                Parameter::new("storage1_sn", ParamKind::Text(None), None, None, 1, 37052, 10, false, true),
            ]),
            ParameterBlock::new(vec![                
                Parameter::new("storage1_total_charge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 37066, 2, false, true),
                Parameter::new("storage1_total_discharge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 37068, 2, false, true),
            ]),
            ParameterBlock::new(vec![    
                Parameter::new("power_meter_status", ParamKind::NumberU16(None), None, None, 1, 37100, 1, false, true),
                Parameter::new("grid_A_voltage", ParamKind::NumberI32(None), None, Some("V"), 10, 37101, 2, false, true),
                Parameter::new("grid_B_voltage", ParamKind::NumberI32(None), None, Some("V"), 10, 37103, 2, false, true),
                Parameter::new("grid_C_voltage", ParamKind::NumberI32(None), None, Some("V"), 10, 37105, 2, false, true),
                Parameter::new("active_grid_A_current", ParamKind::NumberI32(None), None, Some("I"), 100, 37107, 2, false, true),
                Parameter::new("active_grid_B_current", ParamKind::NumberI32(None), None, Some("I"), 100, 37109, 2, false, true),
                Parameter::new("active_grid_C_current", ParamKind::NumberI32(None), None, Some("I"), 100, 37111, 2, false, true),
                Parameter::new("power_meter_active_power", ParamKind::NumberI32(None), None, Some("W"), 1, 37113, 2, false, true),
                Parameter::new("power_meter_reactive_power", ParamKind::NumberI32(None), None, Some("Var"), 1, 37115, 2, false, true),
                Parameter::new("active_grid_power_factor", ParamKind::NumberI16(None), None, None, 1000, 37117, 1, false, true),
                Parameter::new("active_grid_frequency", ParamKind::NumberI16(None), None, Some("Hz"), 100, 37118, 1, false, true),
                Parameter::new("grid_exported_energy", ParamKind::NumberI32(None), None, Some("kWh"), 100, 37119, 2, false, true),
                Parameter::new("power_meter_reverse_active_power", ParamKind::NumberI32(None), None, Some("kWh"), 100, 37121, 2, false, true),
                Parameter::new("power_meter_accumulated_reactive_powe", ParamKind::NumberI32(None), None, Some("kVarH"), 100, 37123, 2, false, true),
                Parameter::new("power_meter_meter_type", ParamKind::NumberU16(None), None, None, 1, 37125, 1, false, true),
                Parameter::new("active_grid_A_B_voltage", ParamKind::NumberI32(None), None, Some("V"), 10, 37126, 2, false, true),
                Parameter::new("active_grid_B_C_voltage", ParamKind::NumberI32(None), None, Some("V"), 10, 37128, 2, false, true),
                Parameter::new("active_grid_C_A_voltage", ParamKind::NumberI32(None), None, Some("V"), 10, 37130, 2, false, true),
                Parameter::new("active_grid_A_power", ParamKind::NumberI32(None), None, Some("W"), 1, 37132, 2, false, true),
                Parameter::new("active_grid_B_power", ParamKind::NumberI32(None), None, Some("W"), 1, 37134, 2, false, true),
                Parameter::new("active_grid_C_power", ParamKind::NumberI32(None), None, Some("W"), 1, 37136, 2, false, true),
            ]),
            ParameterBlock::new(vec![        
                Parameter::new("nb_optimizers", ParamKind::NumberU16(None), None, None, 1, 37200, 1, false, false),
                Parameter::new("nb_online_optimizers", ParamKind::NumberU16(None), None, None, 1, 37201, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("storage_rated_capacity", ParamKind::NumberU32(None), None, Some("Wh"), 1, 37758, 2, false, true),
                Parameter::new("storage_battery_soc", ParamKind::NumberU16(None), None, Some("%"), 10, 37760, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                
                Parameter::new("storage_status", ParamKind::NumberU16(None), None, Some("storage_status_enum"), 1, 37762, 1, false, true),
                Parameter::new("storage_bus_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 37763, 1, false, true),
                Parameter::new("storage_bus_current", ParamKind::NumberI16(None), None, Some("A"), 10, 37764, 1, false, true),
                Parameter::new("storage_charge_discharge_power", ParamKind::NumberI32(None), None, Some("W"), 1, 37765, 2, false, true),
            ]),
            ParameterBlock::new(vec![                
                Parameter::new("storage_current_day_charge_capacity", ParamKind::NumberU32(None), None, Some("kWh"), 100, 37784, 2, false, true),
                Parameter::new("storage_current_day_discharge_capacity", ParamKind::NumberU32(None), None, Some("kWh"), 100, 37786,  2, false, true),
            ]),
            ParameterBlock::new(vec![        
                Parameter::new("storage1_sw_version", ParamKind::Text(None), None, None, 1, 37814, 15, false, true),
            ]),
            ParameterBlock::new(vec![                            
                Parameter::new("storage1_battery1_sn", ParamKind::Text(None), None, None, 1, 38200, 10, false, true),
                Parameter::new("storage1_battery1_sw_version", ParamKind::Text(None), None, None, 1, 38210, 15, false, true),
            ]),
            ParameterBlock::new(vec![                                
                Parameter::new("storage1_battery1_working_status", ParamKind::NumberU16(None), None, Some("storage_status_enum"), 1, 38228, 1, false, true),
                Parameter::new("storage1_battery1_soc", ParamKind::NumberU16(None), None, Some("%"), 10, 38229, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                                    
                Parameter::new("storage1_battery1_charge_discharge_power", ParamKind::NumberI32(None), None, Some("kW"), 1, 38233, 2, false, true),
                Parameter::new("storage1_battery1_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 38235, 1, false, true),
                Parameter::new("storage1_battery1_current", ParamKind::NumberI16(None), None, Some("A"), 10, 38236, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery1_total_charge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 38238, 2, false, true),
                Parameter::new("storage1_battery1_total_discharge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 38240,  2, false, true),
                Parameter::new("storage1_battery2_sn", ParamKind::Text(None), None, None, 1, 38242, 10, false, true),
                Parameter::new("storage1_battery2_sw_version", ParamKind::Text(None), None, None, 1, 38252, 15, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery2_working_status", ParamKind::NumberU16(None), None, Some("storage_status_enum"), 1, 38270, 1, false, true),
                Parameter::new("storage1_battery2_soc", ParamKind::NumberU16(None), None, Some("%"), 10, 38271, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery2_charge_discharge_power", ParamKind::NumberI32(None), None, Some("kW"), 1, 38275, 2, false, true),
                Parameter::new("storage1_battery2_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 38277, 1, false, true),
                Parameter::new("storage1_battery2_current", ParamKind::NumberI16(None), None, Some("A"), 10, 38278, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery2_total_charge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 38280, 2, false, true),
                Parameter::new("storage1_battery2_total_discharge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 38282,  2, false, true),
                Parameter::new("storage1_battery3_sn", ParamKind::Text(None), None, None, 1, 38284, 10, false, true),
                Parameter::new("storage1_battery3_sw_version", ParamKind::Text(None), None, None, 1, 38294, 15, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery3_working_status", ParamKind::NumberU16(None), None, Some("storage_status_enum"), 1, 38312, 1, false, true),
                Parameter::new("storage1_battery3_soc", ParamKind::NumberU16(None), None, Some("%"), 10, 38313, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery3_charge_discharge_power", ParamKind::NumberI32(None), None, Some("kW"), 1, 38317, 2, false, true),
                Parameter::new("storage1_battery3_voltage", ParamKind::NumberU16(None), None, Some("V"), 10, 38319, 1, false, true),
                Parameter::new("storage1_battery3_current", ParamKind::NumberI16(None), None, Some("A"), 10, 38320, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage1_battery3_total_charge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 38322, 2, false, true),
                Parameter::new("storage1_battery3_total_discharge", ParamKind::NumberU32(None), None, Some("kWh"), 100, 38324,  2, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("storage1_battery1_max_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 38452, 1, false, true),
                Parameter::new("storage1_battery1_min_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 38453, 1, false, true),
                Parameter::new("storage1_battery2_max_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 38454, 1, false, true),
                Parameter::new("storage1_battery2_min_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 38455, 1, false, true),
                Parameter::new("storage1_battery3_max_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 38456, 1, false, true),
                Parameter::new("storage1_battery3_min_temperature", ParamKind::NumberI16(None), None, Some("°C"), 10, 38457, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("system_time", ParamKind::NumberU32(None), None, Some("epoch"), 1, 40000, 2, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("grid_code", ParamKind::NumberU16(None), None, Some("grid_enum"), 1, 42000, 1, false, true),
            ]),
            ParameterBlock::new(vec![        
                Parameter::new("time_zone", ParamKind::NumberI16(None), None, Some("min"), 1, 43006, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("storage_working_mode", ParamKind::NumberI16(None), None, Some("storage_working_mode_enum"), 1, 47004, 1, false, true),
            ]),
            ParameterBlock::new(vec![                                    
                Parameter::new("storage_time_of_use_price", ParamKind::NumberI16(None), None, Some("storage_tou_price_enum"), 1, 47027, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("storage_lcoe", ParamKind::NumberU32(None), None, None, 1000, 47069, 2, false, true),
            ]),
            ParameterBlock::new(vec![        
                Parameter::new("storage_maximum_charging_power", ParamKind::NumberU32(None), None, Some("W"), 1, 47075, 2, false, true),
                Parameter::new("storage_maximum_discharging_power", ParamKind::NumberU32(None), None, Some("W"), 1, 47077, 2, false, true),
                Parameter::new("storage_power_limit_grid_tied_point", ParamKind::NumberI32(None), None, Some("W"), 1, 47079, 2, false, true),
                Parameter::new("storage_charging_cutoff_capacity", ParamKind::NumberU16(None), None, Some("%"), 10, 47081, 1, false, true),
                Parameter::new("storage_discharging_cutoff_capacity", ParamKind::NumberU16(None), None, Some("%"), 10, 47082, 1, false, true),
                Parameter::new("storage_forced_charging_and_discharging_period", ParamKind::NumberU16(None), None, Some("min"), 1, 47083, 1, false, true),
                Parameter::new("storage_forced_charging_and_discharging_power", ParamKind::NumberI32(None), None, Some("min"), 1, 47084, 2, false, true),
                Parameter::new("storage_working_mode", ParamKind::NumberU16(None), None, Some("working_mode"), 1, 47086, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("active_power_control_mode", ParamKind::NumberU16(None), None, Some("active_power_control_mode_enum"), 1, 47415, 1, false, true),
            ]),
            ParameterBlock::new(vec![            
                Parameter::new("storage1_battery1_no", ParamKind::NumberU16(None), None, None, 1, 47750, 1, false, true),
                Parameter::new("storage1_battery2_no", ParamKind::NumberU16(None), None, None, 1, 47751, 1, false, true),
                Parameter::new("storage1_battery3_no", ParamKind::NumberU16(None), None, None, 1, 47752, 1, false, true),
        ])
        ]
    }

    async fn save_to_influxdb(
        client: influxdb::Client,
        thread_name: &String,
        param: Parameter,
    ) -> Result<()> {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let mut query = Timestamp::Milliseconds(since_the_epoch).into_query(&param.name);
        query = query.add_field("value", param.get_influx_value());

        match client.query(&query).await {
            Ok(msg) => {
                if msg != "" {
                    error!("{}: influxdb write success: {:?}", thread_name, msg);
                } else {
                    debug!("{}: influxdb write success: {:?}", thread_name, msg);
                }
            }
            Err(e) => {
                error!("<i>{}</>: influxdb write error: <b>{:?}</>", thread_name, e);
            }
        }

        Ok(())
    }

    async fn save_ms_to_influxdb(
        client: influxdb::Client,
        thread_name: &String,
        ms: u64,
        param_count: usize,
    ) -> Result<()> {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let mut query = Timestamp::Milliseconds(since_the_epoch).into_query("inverter_query_time");
        query = query.add_field("value", ms);
        query = query.add_field("param_count", param_count as u8);

        match client.query(&query).await {
            Ok(msg) => {
                if msg != "" {
                    error!("{}: influxdb write success: {:?}", thread_name, msg);
                } else {
                    debug!("{}: influxdb write success: {:?}", thread_name, msg);
                }
            }
            Err(e) => {
                error!("<i>{}</>: influxdb write error: <b>{:?}</>", thread_name, e);
            }
        }

        Ok(())
    }

    async fn read_params(
        &mut self,
        mut ctx: Context,
        parameters: &[ParameterBlock],
        initial_read: bool,
    ) -> io::Result<(Context, Vec<Parameter>)> {
        // connect to influxdb
        let client = match &self.influxdb_url {
            Some(url) => match &self.influxdb_token {
                Some(token) => Some(Client::new(url, "sun2000").with_token(token)),
                None => Some(Client::new(url, "sun2000")),
            },
            None => None,
        };

        let mut params: Vec<Parameter> = vec![];
        let mut disconnected = false;
        let now = Instant::now();

        for pb in parameters {
            if self.partial {
                for p in pb.parameters.iter().filter(|s| {
                    (initial_read && s.initial_read)
                        || (!initial_read
                            && (s.save_to_influx
                                || s.name.starts_with("state_")
                                || s.name.starts_with("alarm_")
                                || s.name.ends_with("_status")
                                || s.name.ends_with("_code")))
                }) {
                    if disconnected {
                        break;
                    }
                    let mut attempts = 0;
                    while attempts < SUN2000_ATTEMPTS_PER_PARAM {
                        attempts += 1;
                        let _ : &Parameter = p;
                        debug!("-> obtaining {} ({:?})...", p.name, p.desc);
                        let retval = ctx.read_holding_registers(p.reg_address, p.len);
                        let read_res;
                        let start = Instant::now();
                        let read_time;
                        match timeout(Duration::from_secs_f32(5.0), retval).await {
                            Ok(res) => {
                                read_res = res;
                                read_time = start.elapsed();
                            }
                            Err(e) => {
                                let msg = format!(
                                    "<i>{}</i>: read timeout (attempt #{} of {}), register: <green><i>{}</>, error: <b>{}</>",
                                    self.name, attempts, SUN2000_ATTEMPTS_PER_PARAM, p.name, e
                                );
                                if attempts == SUN2000_ATTEMPTS_PER_PARAM {
                                    error!("{}", msg);
                                    break;
                                } else {
                                    warn!("{}", msg);
                                    continue;
                                };
                            }
                        }
                        match read_res {
                            Ok(data) => {
                                if read_time > Duration::from_secs_f32(3.5) {
                                    warn!(
                                        "<i>{}</i>: inverter has lagged during read, register: <green><i>{}</>, read time: <b>{:?}</>",
                                        self.name, p.name, read_time
                                    );
                                }
        
                                let mut val;
                                match &p.value {
                                    ParamKind::Text(_) => {
                                        let bytes: Vec<u8> = data.iter().fold(vec![], |mut x, elem| {
                                            if (elem >> 8) as u8 != 0 {
                                                x.push((elem >> 8) as u8);
                                            }
                                            if (elem & 0xff) as u8 != 0 {
                                                x.push((elem & 0xff) as u8);
                                            }
                                            x
                                        });
                                        let id = String::from_utf8(bytes).unwrap();
                                        val = ParamKind::Text(Some(id));
                                    }
                                    ParamKind::NumberU16(_) => {
                                        debug!("-> {} = {:?}", p.name, data);
                                        val = ParamKind::NumberU16(Some(data[0]));
                                    }
                                    ParamKind::NumberI16(_) => {
                                        debug!("-> {} = {:?}", p.name, data);
                                        val = ParamKind::NumberI16(Some(data[0] as i16));
                                    }
                                    ParamKind::NumberU32(_) => {
                                        let new_val: u32 = ((data[0] as u32) << 16) | data[1] as u32;
                                        debug!("-> {} = {:X?} {:X}", p.name, data, new_val);
                                        val = ParamKind::NumberU32(Some(new_val));
                                        if p.unit.unwrap_or_default() == "epoch" && new_val == 0 {
                                            //zero epoch makes no sense, let's set it to None
                                            val = ParamKind::NumberU32(None);
                                        }
                                    }
                                    ParamKind::NumberI32(_) => {
                                        let new_val: i32 =
                                            ((data[0] as i32) << 16) | (data[1] as u32) as i32;
                                        debug!("-> {} = {:X?} {:X}", p.name, data, new_val);
                                        val = ParamKind::NumberI32(Some(new_val));
                                    }
                                }
                                let param = Parameter::new_from_string(
                                    p.name.clone(),
                                    val,
                                    p.desc,
                                    p.unit,
                                    p.gain,
                                    p.reg_address,
                                    p.len,
                                    p.initial_read,
                                    p.save_to_influx,
                                );
                                params.push(param.clone());
        
                                //write data to influxdb if configured
                                if let Some(c) = client.clone() {
                                    if !initial_read && p.save_to_influx {
                                        let _ = Sun2000::save_to_influxdb(c, &self.name, param).await;
                                    }
                                }
        
                                break; //read next parameter
                            }
                            Err(e) => {
                                let msg = format!(
                                    "<i>{}</i>: read error (attempt #{} of {}), register: <green><i>{}</>, error: <b>{}</>, read time: <b>{:?}</>",
                                    self.name, attempts, SUN2000_ATTEMPTS_PER_PARAM, p.name, e, read_time
                                );
                                match e.kind() {
                                    ErrorKind::BrokenPipe | ErrorKind::ConnectionReset => {
                                        error!("{}", msg);
                                        disconnected = true;
                                        break;
                                    }
                                    _ => {
                                        if attempts == SUN2000_ATTEMPTS_PER_PARAM {
                                            error!("{}", msg);
                                            break;
                                        } else {
                                            warn!("{}", msg);
                                            continue;
                                        };
                                    }
                                }
                            }
                        }
                    }
                }        
            } else {
                if disconnected {
                    break;
                }
                let mut attempts = 0;
                while attempts < SUN2000_ATTEMPTS_PER_PARAM {
                    attempts += 1;
                    debug!("-> obtaining block {}...", pb.reg_address);
                    let retval = ctx.read_holding_registers(pb.reg_address, pb.len);
                    let read_res;
                    let start = Instant::now();
                    let read_time;
                    match timeout(Duration::from_secs_f32(5.0), retval).await {
                        Ok(res) => {
                            read_res = res;
                            read_time = start.elapsed();
                        }
                        Err(e) => {
                            let msg = format!(
                                "<i>{}</i>: read timeout (attempt #{} of {}), register: <green><i>{}</>, error: <b>{}</>",
                                self.name, attempts, SUN2000_ATTEMPTS_PER_PARAM, pb.reg_address, e
                            );
                            if attempts == SUN2000_ATTEMPTS_PER_PARAM {
                                error!("{}", msg);
                                break;
                            } else {
                                warn!("{}", msg);
                                continue;
                            };
                        }
                    }
                    match read_res {
                        Ok(fullv) => {
                            let mut remaining_data = &fullv[..]; 
                            if read_time > Duration::from_secs_f32(3.5) {
                                warn!(
                                    "<i>{}</i>: inverter has lagged during read, register: <green><i>{}</>, read time: <b>{:?}</>",
                                    self.name, pb.reg_address, read_time
                                );
                            }

                            for p in pb.parameters.iter().filter(|s| {
                                (initial_read && s.initial_read)
                                    || (!initial_read
                                        && (s.save_to_influx
                                            || s.name.starts_with("state_")
                                            || s.name.starts_with("alarm_")
                                            || s.name.ends_with("_status")
                                            || s.name.ends_with("_code")))
                            }) {
                                let data = &remaining_data[0..(p.len as usize)];
                                remaining_data = &remaining_data[(p.len as usize)..];
                                let mut val;
                                match &p.value {
                                    ParamKind::Text(_) => {
                                        let bytes: Vec<u8> = data.iter().fold(vec![], |mut x, elem| {
                                            if (elem >> 8) as u8 != 0 {
                                                x.push((elem >> 8) as u8);
                                            }
                                            if (elem & 0xff) as u8 != 0 {
                                                x.push((elem & 0xff) as u8);
                                            }
                                            x
                                        });
                                        let id = String::from_utf8(bytes).unwrap();
                                        val = ParamKind::Text(Some(id));
                                    }
                                    ParamKind::NumberU16(_) => {
                                        debug!("-> {} = {:?}", p.name, data);
                                        val = ParamKind::NumberU16(Some(data[0]));
                                    }
                                    ParamKind::NumberI16(_) => {
                                        debug!("-> {} = {:?}", p.name, data);
                                        val = ParamKind::NumberI16(Some(data[0] as i16));
                                    }
                                    ParamKind::NumberU32(_) => {
                                        let new_val: u32 = ((data[0] as u32) << 16) | data[1] as u32;
                                        debug!("-> {} = {:X?} {:X}", p.name, data, new_val);
                                        val = ParamKind::NumberU32(Some(new_val));
                                        if p.unit.unwrap_or_default() == "epoch" && new_val == 0 {
                                            //zero epoch makes no sense, let's set it to None
                                            val = ParamKind::NumberU32(None);
                                        }
                                    }
                                    ParamKind::NumberI32(_) => {
                                        let new_val: i32 =
                                            ((data[0] as i32) << 16) | (data[1] as u32) as i32;
                                        debug!("-> {} = {:X?} {:X}", p.name, data, new_val);
                                        val = ParamKind::NumberI32(Some(new_val));
                                    }
                                }
                                let param = Parameter::new_from_string(
                                    p.name.clone(),
                                    val,
                                    p.desc,
                                    p.unit,
                                    p.gain,
                                    p.reg_address,
                                    p.len,
                                    p.initial_read,
                                    p.save_to_influx,
                                );
                                params.push(param.clone());
        
                                //write data to influxdb if configured
                                if let Some(c) = client.clone() {
                                    if !initial_read && p.save_to_influx {
                                        let _ = Sun2000::save_to_influxdb(c, &self.name, param).await;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let msg = format!(
                                "<i>{}</i>: read error (attempt #{} of {}), register: <green><i>{}</>, error: <b>{}</>, read time: <b>{:?}</>",
                                self.name, attempts, SUN2000_ATTEMPTS_PER_PARAM, pb.reg_address, e, read_time
                            );
                            match e.kind() {
                                ErrorKind::BrokenPipe | ErrorKind::ConnectionReset => {
                                    error!("{}", msg);
                                    disconnected = true;
                                    break;
                                }
                                _ => {
                                    if attempts == SUN2000_ATTEMPTS_PER_PARAM {
                                        error!("{}", msg);
                                        break;
                                    } else {
                                        warn!("{}", msg);
                                        continue;
                                    };
                                }
                            }
                        }
                    }
                    break;
                }  
            }
        }

        let elapsed = now.elapsed();
        let ms = (elapsed.as_secs() * 1_000) + elapsed.subsec_millis() as u64;
        debug!(
            "{}: read {} parameters [⏱️ {} ms]",
            self.name,
            params.len(),
            ms
        );

        //save query time
        if let Some(c) = client {
            let _ = Sun2000::save_ms_to_influxdb(c, &self.name, ms, params.len()).await;
        }
        Ok((ctx, params))
    }

    pub fn attribute_parser(&self, mut a: Vec<u8>) -> Result<()> {
        //search for 'Description about the first device' (0x88)
        if let Some(index) = a.iter().position(|&x| x == 0x88) {
            //strip beginning bytes up to descriptor start
            a.drain(0..=index);

            //next (first) byte is len
            let len = a[0] as usize;

            //leave only the relevant descriptor string
            a = a.drain(1..=len).collect();

            //convert it to string
            let x = String::from_utf8(a)?;

            //split by semicolons
            let split = x.split(';');

            //parse and dump all attributes
            info!(
                "<i>{}</i>: <blue>Device Description attributes:</>",
                self.name
            );
            for s in split {
                let mut sp = s.split('=');
                let id = sp.next();
                let val = sp.next();
                if id.is_none() || val.is_none() {
                    continue;
                }
                info!(
                    "<i>{}</i>: <bright-black>{}:</> {}: <b><cyan>{}</>",
                    self.name,
                    id.unwrap(),
                    get_attribute_name(id.unwrap()),
                    val.unwrap()
                );
            }
        }
        Ok(())
    }

    #[rustfmt::skip]
    pub async fn worker(&mut self, worker_cancel_flag: Arc<AtomicBool>) -> Result<()> {
        info!("<i>{}</>: Starting task", self.name);
        let mut poll_interval = Instant::now();
        let mut stats_interval = Instant::now();
        let mut terminated = false;

        loop {
            if terminated || worker_cancel_flag.load(Ordering::SeqCst) {
                break;
            }

            let socket_addr = self.host_port.parse().unwrap();

            let slave = if self.dongle_connection {
                //USB dongle connection: Slave ID has to be 0x01
                Slave(0x01)
            } else {
                //internal wifi: Slave ID has to be 0x00, otherwise the inverter is not responding
                Slave(0x00)
            };

            info!("<i>{}</>: connecting to <u>{}</>...", self.name, self.host_port);
            let retval = tcp::connect_slave(socket_addr, slave);
            let conn= match timeout(Duration::from_secs(5), retval).await {
                Ok(res) => res,
                Err(e) => {
                    error!("<i>{}</>: connect timeout: <b>{}</>", self.name, e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            match conn {
                Ok(mut ctx) => {
                    info!("<i>{}</>: connected successfully", self.name);
                    //initial parameters table
                    let parameters = Sun2000::param_table();
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    //obtaining all parameters from inverter
                    let (new_ctx, params) = self.read_params(ctx, &parameters, true).await?;
                    ctx = new_ctx;
                    for p in &params {
                        match &p.value {
                            ParamKind::Text(_) => match p.name.as_ref() {
                                "model_name" => {
                                    info!("<i>{}</>: model name: <b><cyan>{}</>", self.name, &p.get_text_value());
                                }
                                "serial_number" => {
                                    info!("<i>{}</>: serial number: <b><cyan>{}</>", self.name, &p.get_text_value());
                                }
                                "product_number" => {
                                    info!("<i>{}</>: product number: <b><cyan>{}</>", self.name, &p.get_text_value());
                                }
                                _ => {}
                            },
                            ParamKind::NumberU32(_) => if p.name == "rated_power" {
                                info!(
                                    "<i>{}</>: rated power: <b><cyan>{} {}</>",
                                    self.name,
                                    &p.get_text_value(),
                                    p.unit.unwrap_or_default()
                                );
                            },
                            _ => {}
                        }
                    }

                    // obtain Device Description Definition
                    use tokio_modbus::prelude::*;
                    let retval = ctx.call(Request::Custom(0x2b, vec![0x0e, 0x03, 0x87]));
                    match timeout(Duration::from_secs_f32(5.0), retval).await {
                        Ok(res) => match res {
                            Ok(rsp) => match rsp {
                                Response::Custom(f, rsp) => {
                                    debug!("<i>{}</>: Result for function {} is '{:?}'", self.name, f, rsp);
                                    let _ = self.attribute_parser(rsp);
                                }
                                _ => {
                                    error!("<i>{}</>: unexpected Reading Device Identifiers (0x2B) result", self.name);
                                }
                            },
                            Err(e) => {
                                warn!("<i>{}</i>: read error during <green><i>Reading Device Identifiers (0x2B)</>, error: <b>{}</>", self.name, e);
                            }
                        },
                        Err(e) => {
                            warn!("<i>{}</i>: read timeout during <green><i>Reading Device Identifiers (0x2B)</>, error: <b>{}</>", self.name, e);
                        }
                    }

                    let mut daily_yield_energy: Option<u32> = None;
                    loop {
                        if worker_cancel_flag.load(Ordering::SeqCst) {
                            debug!("<i>{}</>: Got terminate signal from main", self.name);
                            terminated = true;
                        }

                        if terminated
                            || stats_interval.elapsed()
                                > Duration::from_secs_f32(SUN2000_STATS_DUMP_INTERVAL_SECS)
                        {
                            stats_interval = Instant::now();
                            info!(
                                "<i>{}</>: 📊 inverter query statistics: ok: <b>{}</>, errors: <b>{}</>, daily energy yield: <b>{:.1} kWh</>",
                                self.name, self.poll_ok, self.poll_errors,
                                daily_yield_energy.unwrap_or_default() as f64 / 100.0,
                            );

                            if terminated {
                                break;
                            }
                        }

                        if poll_interval.elapsed()
                            > Duration::from_secs_f32(SUN2000_POLL_INTERVAL_SECS)
                        {
                            poll_interval = Instant::now();
                            // let mut active_power: Option<i32> = None;

                            //obtaining all parameters from inverter
                            let (new_ctx, params) =
                                self.read_params(ctx, &parameters, false).await?;
                            ctx = new_ctx;
                            for p in &params {
                                if let ParamKind::NumberU32(n) = p.value { if p.name == "daily_yield_energy" { daily_yield_energy = n } }
                            }

                            let param_count = parameters.iter().map(|x| x.parameters.iter()).flatten().filter(|s| s.save_to_influx ||
                                s.name.starts_with("state_") ||
                                s.name.starts_with("alarm_") ||
                                s.name.ends_with("_status") ||
                                s.name.ends_with("_code")).count();
                            if params.len() != param_count {
                                error!("<i>{}</>: problem obtaining a complete parameter list (read: {}, expected: {}), reconnecting...", self.name, params.len(), param_count);
                                self.poll_errors += 1;
                                break;
                            } else {
                                self.poll_ok += 1;
                            }

                            //process obtained parameters
                            debug!("Query complete, dump results:");
                            for p in &params {
                                debug!(
                                    "  {} ({:?}): {} {}",
                                    p.name,
                                    p.desc.unwrap_or_default(),
                                    p.get_text_value(),
                                    p.unit.unwrap_or_default()
                                );
                            }
                        }

                        tokio::time::sleep(Duration::from_millis(30)).await;
                    }
                }
                Err(e) => {
                    error!("<i>{}</>: connection error: <b>{}</>", self.name, e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }

        info!("{}: task stopped", self.name);
        Ok(())
    }
}
