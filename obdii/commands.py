from obd import OBDCommand
from obd.protocols import ECU
from obd.decoders import raw_string, percent
from decoders import *

# flake8: noqa

ext_commands = {
#                                          name                       description                          cmd           bytes       decoder              ECU          fast
    'CAN_HEADER_7E0':          OBDCommand("CAN_HEADER_7E0",          "Set CAN module ID to 7E0"          , b"ATSH7E0" ,  0,          raw_string          ,ECU.UNKNOWN, False),
    'CAN_HEADER_7E1':          OBDCommand("CAN_HEADER_7E1",          "Set CAN module ID to 7E1"          , b"ATSH7E1" ,  0,          raw_string          ,ECU.UNKNOWN, False),
    'CAN_HEADER_7E4':          OBDCommand("CAN_HEADER_7E4",          "Set CAN module ID to 7E4"          , b"ATSH7E4" ,  0,          raw_string          ,ECU.UNKNOWN, False),
    'CAN_HEADER_7E7':          OBDCommand("CAN_HEADER_7E7",          "Set CAN module ID to 7E7"          , b"ATSH7E7" ,  0,          raw_string          ,ECU.UNKNOWN, False),

    'CAN_RECEIVE_ADDRESS_7E8': OBDCommand("CAN_RECEIVE_ADDRESS_7E8", "Set the CAN receive address to 7E8"          , b"ATCRA7E8",  0, raw_string          , ECU.UNKNOWN, False),
    'CAN_RECEIVE_ADDRESS_7E9': OBDCommand("CAN_RECEIVE_ADDRESS_7E9", "Set the CAN receive address to 7E9"          , b"ATCRA7E9",  0, raw_string          , ECU.UNKNOWN, False),
    'CAN_RECEIVE_ADDRESS_7EC': OBDCommand("CAN_RECEIVE_ADDRESS_7EC", "Set the CAN receive address to 7EC"          , b"ATCRA7EC",  0, raw_string          , ECU.UNKNOWN, False),
    'CAN_RECEIVE_ADDRESS_7EF': OBDCommand("CAN_RECEIVE_ADDRESS_7EF", "Set the CAN receive address to 7EF"          , b"ATCRA7EF",  0, raw_string          , ECU.UNKNOWN, False),

    'BAT_PACK_CAP_AH_RAW_2018':       OBDCommand("BAT_PACK_CAP_AH_RAW_2018",            "Battery Capacity Ah Raw 2017 - 2018"            , b"2241a3"     ,  0, bat_ah_raw_2018,     ECU.ALL    , False),  
    'BAT_PACK_CAP_AH_RAW_2019':       OBDCommand("BAT_PACK_CAP_AH_RAW_2019",            "Battery Capacity Ah Raw 2019+"                  , b"2245f9"     ,  0, bat_ah_raw_2019,     ECU.ALL    , False),  
    'BAT_PACK_CAP_AH_EST_2018':       OBDCommand("BAT_PACK_CAP_KWH_EST_2018",           "Battery Capacity KWh Est 2017 - 2018"           , b"2241a3"     ,  0, bat_kwh_raw_2018,    ECU.ALL    , False),  
    'BAT_PACK_CAP_AH_EST_2019':       OBDCommand("BAT_PACK_CAP_KWH_EST_2019",           "Battery Capacity KWh Est 2019+"                 , b"2245f9"     ,  0, bat_kwh_raw_2019,    ECU.ALL    , False),  
    'BAT_PACK_SOC_DISP':              OBDCommand("BAT_PACK_SOC_DISP",                   "Battery SoC Displayed"                          , b"228334"     ,  0, bat_soc_disp,        ECU.ALL    , False), 
    'BAT_PACK_SOC_RAW_HD':            OBDCommand("BAT_PACK_SOC_RAW_HD",                 "Battery SoC Raw HD"                             , b"2243af"     ,  0, bat_soc_raw_hd,      ECU.ALL    , False), 
    'BAT_PACK_SOC_RAW_LD':            OBDCommand("BAT_PACK_SOC_RAW_LD",                 "Battery SoC Raw LD"                             , b"015b"     ,  0, bat_soc_raw_ld_var,      ECU.ALL    , False), 
    'BAT_PACK_SOC_RAW_LD2':            OBDCommand("BAT_PACK_SOC_RAW_LD2",                 "Battery SoC Raw LD2"                             , b"222411"     ,  0, bat_soc_raw_ld_var,      ECU.ALL    , False), 
    'BAT_PACK_SOC_RAW_LD3':            OBDCommand("BAT_PACK_SOC_RAW_LD3",                 "Battery SoC Raw LD3"                             , b"22432f"     ,  0, bat_soc_raw_ld_var,      ECU.ALL    , False), 
    'BAT_PACK_SOC_VAR':            OBDCommand("BAT_PACK_SOC_VAR",                 "Battery SoC Variation"                             , b"22435f"     ,  0, bat_soc_raw_ld_var,      ECU.ALL    , False), 
    'BAT_PACK_CURRENT_HD':            OBDCommand("BAT_PACK_CURRENT_HD",                 "Battery Current HD"                             , b"2240d4"     ,  0, bat_current_hd,      ECU.ALL    , False), 
    
    'AMBIENT_AIR_TEMP':                  OBDCommand("AMBIENT_AIR_TEMP",                "Ambient Air Temp"          , b"220046"    ,  0, coolant_temp, ECU.ALL    , False),  
    'BAT_COOLANT_TEMP':                  OBDCommand("BAT_COOLANT_TEMP",                "Battery Coolant Temp"          , b"2241A4"    ,  0, coolant_temp, ECU.ALL    , False),  
    'ELEC_COOLANT_TEMP':                 OBDCommand("ELEC_COOLANT_TEMP",                "Electronics Coolant Temp"          , b"2241A4"    ,  0, coolant_temp, ECU.ALL    , False),  
    'AC_VOLTAGE':              OBDCommand("AC_VOLTAGE",                "AC Voltage"          , b"224368"    ,  0, ac_voltage, ECU.ALL    , False), 
    'AC_CURRENT':              OBDCommand("AC_CURRENT",                "AC Current"          , b"224369"    ,  0, ac_current, ECU.ALL    , False), 
    'CHARGING_LEVEL':              OBDCommand("CHARGING_LEVEL",                "Charging Level"          , b"224531"    ,  0, charging_level, ECU.ALL    , False), 
   

}
