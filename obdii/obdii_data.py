#!/usr/bin/env python3

import ssl
import time
import json
import os
import logging
import logging.handlers
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
import obd
from obd import OBDStatus

from commands import ext_commands


class OBDIIConnectionError(Exception):
    pass


class CanError(Exception):
    pass


def obd_connect(portstr, baudrate, fast=False, timeout=30, max_attempts=3):
    connection_count = 0
    obd_connection = None
    while (obd_connection is None or obd_connection.status() != OBDStatus.CAR_CONNECTED) and connection_count < max_attempts:
        connection_count += 1
        # Establish connection with OBDII dongle
        obd_connection = obd.OBD(portstr=portstr,
                                 baudrate=baudrate,
                                 fast=fast,
                                 timeout=timeout)
        if (obd_connection is None or obd_connection.status() != OBDStatus.CAR_CONNECTED) and connection_count < max_attempts:
            logger.warning("{}. Retrying in {} second(s)...".format(obd_connection.status(), connection_count))
            time.sleep(connection_count)

    if obd_connection.status() != OBDStatus.CAR_CONNECTED:
        raise OBDIIConnectionError(obd_connection.status())
    else:
        return obd_connection


def query_command(connection, command, max_attempts=3):
    command_count = 0
    cmd_response = None
    exception = False
    valid_response = False
    while not valid_response and command_count < max_attempts:
        command_count += 1
        try:
            cmd_response = connection.query(command, force=True)
        except Exception:
            exception = True
        valid_response = not(cmd_response is None or cmd_response.is_null() or cmd_response.value is None or cmd_response.value == "?" or cmd_response.value == "" or exception)
        if not valid_response and command_count < max_attempts:
            logger.warning("No valid response for {}. Retrying in {} second(s)...".format(command, command_count))
            time.sleep(command_count)

    if not valid_response:
        raise ValueError("No valid response for {}. Max attempts ({}) exceeded."
                         .format(command, max_attempts))
    else:
        logger.info("Got response from command: {} ".format(command))
        return cmd_response

def query_charging_level(connection):
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["CHARGING_LEVEL"],3)

    return resp.value

#########################################################


def query_bat_pack_cap_ah_raw_2018(connection):
    logger.info("**** Querying bat_pack_cap_ah_raw_2018 information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_CAP_AH_RAW_2018"],3)

    return resp.value


def query_bat_pack_cap_ah_raw_2019(connection):
    logger.info("**** Querying bat_pack_cap_ah_raw_2019 information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_CAP_AH_RAW_2019"],3)

    return resp.value


def query_bat_pack_cap_kwh_est_2018(connection):
    logger.info("**** Querying bat_pack_cap_kwh_est_2018 information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_CAP_KWH_EST_2018"],3)

    return resp.value


def query_bat_pack_cap_kwh_est_2019(connection):
    logger.info("**** Querying bat_pack_cap_kwh_est_2019 information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_CAP_KWH_EST_2019"],3)

    return resp.value


def query_bat_pack_soc_disp(connection):
    logger.info("**** Querying bat_pack_soc_disp information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_SOC_DISP"],3)

    return resp.value


def query_bat_pack_soc_raw_hd(connection):
    logger.info("**** Querying bat_pack_soc_raw_hd information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_SOC_RAW_HD"],3)

    return resp.value


def query_bat_pack_soc_raw_ld(connection):
    logger.info("**** Querying bat_pack_soc_raw_ld information ****")
    # Set header to 7E0
    query_command(connection, ext_commands["CAN_HEADER_7E0"])
    # Set the CAN receive address to 7E8
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7E8"])

    resp = query_command(connection, ext_commands["BAT_PACK_SOC_RAW_LD"],3)

    return resp.value


def query_bat_pack_soc_raw_ld2(connection):
    logger.info("**** Querying bat_pack_soc_raw_ld2 information ****")
    # Set header to 7E1
    query_command(connection, ext_commands["CAN_HEADER_7E1"])
    # Set the CAN receive address to 7E9
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7E9"])

    resp = query_command(connection, ext_commands["BAT_PACK_SOC_RAW_LD2"],3)

    return resp.value


def query_bat_pack_soc_raw_ld3(connection):
    logger.info("**** Querying bat_pack_soc_raw_ld3 information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_SOC_RAW_LD3"],3)

    return resp.value


def query_bat_pack_soc_var(connection):
    logger.info("**** Querying bat_pack_soc_var information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_SOC_VAR"],3)

    return resp.value


def query_bat_pack_current_hd(connection):
    logger.info("**** Querying bat_pack_current_hd information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_PACK_CURRENT_HD"],3)

    return resp.value


def query_bat_pack_num_charges(connection):
    logger.info("**** Querying bat_pack_num_charges information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_NUM_CHARGES"],3)

    return resp.value


def query_bat_mod_temp_max(connection):
    logger.info("**** Querying bat_mod_temp_max information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_MAX"],3)

    return resp.value


def query_bat_mod_temp_min(connection):
    logger.info("**** Querying bat_mod_temp_min information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_MIN"],3)

    return resp.value


def query_bat_mod_temp_avg(connection):
    logger.info("**** Querying bat_mod_temp_avg information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_AVG"],3)

    return resp.value


def query_bat_cell_volt_min(connection):
    logger.info("**** Querying bat_cell_volt_min information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_MIN"],3)

    return resp.value


def query_bat_cell_volt_min_num(connection):
    logger.info("**** Querying bat_cell_volt_min_num information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_MIN_NUM"],3)

    return resp.value


def query_bat_cell_volt_max(connection):
    logger.info("**** Querying bat_cell_volt_max information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_MAX"],3)

    return resp.value


def query_bat_cell_volt_max_num(connection):
    logger.info("**** Querying bat_cell_volt_max_num information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_MAX_NUM"],3)

    return resp.value


def query_bat_pack_resistance(connection):
    logger.info("**** Querying bat_pack_resistance information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_RESISTANCE"],3)

    return resp.value


def query_bat_pack_volt_min(connection):
    logger.info("**** Querying bat_pack_volt_min information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_VOLT_MIN"],3)

    return resp.value


def query_bat_pack_volt_max(connection):
    logger.info("**** Querying bat_pack_volt_max information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_PACK_VOLT_MAX"],3)

    return resp.value


def query_hv_current(connection):
    logger.info("**** Querying hv_current information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["HV_CURRENT"],3)

    return resp.value


def query_ambient_air_temp(connection):
    logger.info("**** Querying ambient_air_temp information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["AMBIENT_AIR_TEMP"],3)

    return resp.value


def query_hv_current_hd(connection):
    logger.info("**** Querying hv_current_hd information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["HV_CURRENT_HD"],3)

    return resp.value


def query_bat_mod_temp_1(connection):
    logger.info("**** Querying bat_mod_temp_1 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_1"],3)

    return resp.value


def query_bat_mod_temp_2(connection):
    logger.info("**** Querying bat_mod_temp_2 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_2"],3)

    return resp.value


def query_bat_mod_temp_3(connection):
    logger.info("**** Querying bat_mod_temp_3 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_3"],3)

    return resp.value


def query_bat_mod_temp_4(connection):
    logger.info("**** Querying bat_mod_temp_4 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_4"],3)

    return resp.value


def query_bat_mod_temp_5(connection):
    logger.info("**** Querying bat_mod_temp_5 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_5"],3)

    return resp.value


def query_bat_mod_temp_6(connection):
    logger.info("**** Querying bat_mod_temp_6 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_MOD_TEMP_6"],3)

    return resp.value


def query_bat_cell_volt_avg(connection):
    logger.info("**** Querying bat_cell_volt_avg information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_AVG"],3)

    return resp.value


def query_bat_cell_volt_01(connection):
    logger.info("**** Querying bat_cell_volt_01 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_01"],3)

    return resp.value


def query_bat_cell_volt_02(connection):
    logger.info("**** Querying bat_cell_volt_02 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_02"],3)

    return resp.value


def query_bat_cell_volt_03(connection):
    logger.info("**** Querying bat_cell_volt_03 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_03"],3)

    return resp.value


def query_bat_cell_volt_04(connection):
    logger.info("**** Querying bat_cell_volt_04 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_04"],3)

    return resp.value


def query_bat_cell_volt_05(connection):
    logger.info("**** Querying bat_cell_volt_05 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_05"],3)

    return resp.value


def query_bat_cell_volt_06(connection):
    logger.info("**** Querying bat_cell_volt_06 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_06"],3)

    return resp.value


def query_bat_cell_volt_07(connection):
    logger.info("**** Querying bat_cell_volt_07 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_07"],3)

    return resp.value


def query_bat_cell_volt_08(connection):
    logger.info("**** Querying bat_cell_volt_08 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_08"],3)

    return resp.value


def query_bat_cell_volt_09(connection):
    logger.info("**** Querying bat_cell_volt_09 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_09"],3)

    return resp.value


def query_bat_cell_volt_10(connection):
    logger.info("**** Querying bat_cell_volt_10 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_10"],3)

    return resp.value


def query_bat_cell_volt_11(connection):
    logger.info("**** Querying bat_cell_volt_11 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_11"],3)

    return resp.value


def query_bat_cell_volt_12(connection):
    logger.info("**** Querying bat_cell_volt_12 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_12"],3)

    return resp.value


def query_bat_cell_volt_13(connection):
    logger.info("**** Querying bat_cell_volt_13 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_13"],3)

    return resp.value


def query_bat_cell_volt_14(connection):
    logger.info("**** Querying bat_cell_volt_14 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_14"],3)

    return resp.value


def query_bat_cell_volt_15(connection):
    logger.info("**** Querying bat_cell_volt_15 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_15"],3)

    return resp.value


def query_bat_cell_volt_16(connection):
    logger.info("**** Querying bat_cell_volt_16 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_16"],3)

    return resp.value


def query_bat_cell_volt_17(connection):
    logger.info("**** Querying bat_cell_volt_17 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_17"],3)

    return resp.value


def query_bat_cell_volt_18(connection):
    logger.info("**** Querying bat_cell_volt_18 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_18"],3)

    return resp.value


def query_bat_cell_volt_19(connection):
    logger.info("**** Querying bat_cell_volt_19 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_19"],3)

    return resp.value


def query_bat_cell_volt_20(connection):
    logger.info("**** Querying bat_cell_volt_20 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_20"],3)

    return resp.value


def query_bat_cell_volt_21(connection):
    logger.info("**** Querying bat_cell_volt_21 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_21"],3)

    return resp.value


def query_bat_cell_volt_22(connection):
    logger.info("**** Querying bat_cell_volt_22 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_22"],3)

    return resp.value


def query_bat_cell_volt_23(connection):
    logger.info("**** Querying bat_cell_volt_23 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_23"],3)

    return resp.value


def query_bat_cell_volt_24(connection):
    logger.info("**** Querying bat_cell_volt_24 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_24"],3)

    return resp.value


def query_bat_cell_volt_25(connection):
    logger.info("**** Querying bat_cell_volt_25 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_25"],3)

    return resp.value


def query_bat_cell_volt_26(connection):
    logger.info("**** Querying bat_cell_volt_26 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_26"],3)

    return resp.value


def query_bat_cell_volt_27(connection):
    logger.info("**** Querying bat_cell_volt_27 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_27"],3)
    return resp.value


def query_bat_cell_volt_28(connection):
    logger.info("**** Querying bat_cell_volt_28 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_28"],3)

    return resp.value


def query_bat_cell_volt_29(connection):
    logger.info("**** Querying bat_cell_volt_29 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_29"],3)

    return resp.value


def query_bat_cell_volt_30(connection):
    logger.info("**** Querying bat_cell_volt_30 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_30"],3)

    return resp.value


def query_bat_cell_volt_31(connection):
    logger.info("**** Querying bat_cell_volt_31 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_31"],3)

    return resp.value


def query_bat_cell_volt_32(connection):
    logger.info("**** Querying bat_cell_volt_32 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_32"],3)

    return resp.value


def query_bat_cell_volt_33(connection):
    logger.info("**** Querying bat_cell_volt_33 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_33"],3)

    return resp.value


def query_bat_cell_volt_34(connection):
    logger.info("**** Querying bat_cell_volt_34 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_34"],3)

    return resp.value


def query_bat_cell_volt_35(connection):
    logger.info("**** Querying bat_cell_volt_35 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_35"],3)

    return resp.value


def query_bat_cell_volt_36(connection):
    logger.info("**** Querying bat_cell_volt_36 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_36"],3)

    return resp.value


def query_bat_cell_volt_37(connection):
    logger.info("**** Querying bat_cell_volt_37 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_37"],3)

    return resp.value


def query_bat_cell_volt_38(connection):
    logger.info("**** Querying bat_cell_volt_38 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_38"],3)

    return resp.value


def query_bat_cell_volt_39(connection):
    logger.info("**** Querying bat_cell_volt_39 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_39"],3)

    return resp.value


def query_bat_cell_volt_40(connection):
    logger.info("**** Querying bat_cell_volt_40 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_40"],3)

    return resp.value


def query_bat_cell_volt_41(connection):
    logger.info("**** Querying bat_cell_volt_41 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_41"],3)

    return resp.value


def query_bat_cell_volt_42(connection):
    logger.info("**** Querying bat_cell_volt_42 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_42"],3)

    return resp.value


def query_bat_cell_volt_43(connection):
    logger.info("**** Querying bat_cell_volt_43 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_43"],3)

    return resp.value


def query_bat_cell_volt_44(connection):
    logger.info("**** Querying bat_cell_volt_44 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_44"],3)

    return resp.value


def query_bat_cell_volt_45(connection):
    logger.info("**** Querying bat_cell_volt_45 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_45"],3)

    return resp.value


def query_bat_cell_volt_46(connection):
    logger.info("**** Querying bat_cell_volt_46 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_46"],3)

    return resp.value


def query_bat_cell_volt_47(connection):
    logger.info("**** Querying bat_cell_volt_47 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_47"],3)

    return resp.value


def query_bat_cell_volt_48(connection):
    logger.info("**** Querying bat_cell_volt_48 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_48"],3)

    return resp.value


def query_bat_cell_volt_49(connection):
    logger.info("**** Querying bat_cell_volt_49 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_49"],3)

    return resp.value


def query_bat_cell_volt_50(connection):
    logger.info("**** Querying bat_cell_volt_50 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_50"],3)

    return resp.value


def query_bat_cell_volt_51(connection):
    logger.info("**** Querying bat_cell_volt_51 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_51"],3)

    return resp.value


def query_bat_cell_volt_52(connection):
    logger.info("**** Querying bat_cell_volt_52 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_52"],3)

    return resp.value


def query_bat_cell_volt_53(connection):
    logger.info("**** Querying bat_cell_volt_53 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_53"],3)

    return resp.value


def query_bat_cell_volt_54(connection):
    logger.info("**** Querying bat_cell_volt_54 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_54"],3)

    return resp.value


def query_bat_cell_volt_55(connection):
    logger.info("**** Querying bat_cell_volt_55 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_55"],3)

    return resp.value


def query_bat_cell_volt_56(connection):
    logger.info("**** Querying bat_cell_volt_56 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_56"],3)

    return resp.value


def query_bat_cell_volt_57(connection):
    logger.info("**** Querying bat_cell_volt_57 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_57"],3)

    return resp.value


def query_bat_cell_volt_58(connection):
    logger.info("**** Querying bat_cell_volt_58 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_58"],3)

    return resp.value


def query_bat_cell_volt_59(connection):
    logger.info("**** Querying bat_cell_volt_59 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_59"],3)

    return resp.value


def query_bat_cell_volt_60(connection):
    logger.info("**** Querying bat_cell_volt_60 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_60"],3)

    return resp.value


def query_bat_cell_volt_61(connection):
    logger.info("**** Querying bat_cell_volt_61 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_61"],3)

    return resp.value


def query_bat_cell_volt_62(connection):
    logger.info("**** Querying bat_cell_volt_62 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_62"],3)

    return resp.value


def query_bat_cell_volt_63(connection):
    logger.info("**** Querying bat_cell_volt_63 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_63"],3)

    return resp.value


def query_bat_cell_volt_64(connection):
    logger.info("**** Querying bat_cell_volt_64 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_64"],3)

    return resp.value


def query_bat_cell_volt_65(connection):
    logger.info("**** Querying bat_cell_volt_65 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_65"],3)

    return resp.value


def query_bat_cell_volt_66(connection):
    logger.info("**** Querying bat_cell_volt_66 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_66"],3)

    return resp.value


def query_bat_cell_volt_67(connection):
    logger.info("**** Querying bat_cell_volt_67 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_67"],3)

    return resp.value


def query_bat_cell_volt_68(connection):
    logger.info("**** Querying bat_cell_volt_68 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_68"],3)

    return resp.value


def query_bat_cell_volt_69(connection):
    logger.info("**** Querying bat_cell_volt_69 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_69"],3)

    return resp.value


def query_bat_cell_volt_70(connection):
    logger.info("**** Querying bat_cell_volt_70 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_70"],3)

    return resp.value


def query_bat_cell_volt_71(connection):
    logger.info("**** Querying bat_cell_volt_71 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_71"],3)

    return resp.value


def query_bat_cell_volt_72(connection):
    logger.info("**** Querying bat_cell_volt_72 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_72"],3)

    return resp.value


def query_bat_cell_volt_73(connection):
    logger.info("**** Querying bat_cell_volt_73 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_73"],3)

    return resp.value


def query_bat_cell_volt_74(connection):
    logger.info("**** Querying bat_cell_volt_74 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_74"],3)

    return resp.value


def query_bat_cell_volt_75(connection):
    logger.info("**** Querying bat_cell_volt_75 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_75"],3)

    return resp.value


def query_bat_cell_volt_76(connection):
    logger.info("**** Querying bat_cell_volt_76 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_76"],3)

    return resp.value


def query_bat_cell_volt_77(connection):
    logger.info("**** Querying bat_cell_volt_77 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_77"],3)

    return resp.value


def query_bat_cell_volt_78(connection):
    logger.info("**** Querying bat_cell_volt_78 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_78"],3)

    return resp.value


def query_bat_cell_volt_79(connection):
    logger.info("**** Querying bat_cell_volt_79 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_79"],3)

    return resp.value


def query_bat_cell_volt_80(connection):
    logger.info("**** Querying bat_cell_volt_80 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_80"],3)

    return resp.value


def query_bat_cell_volt_81(connection):
    logger.info("**** Querying bat_cell_volt_81 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_81"],3)

    return resp.value


def query_bat_cell_volt_82(connection):
    logger.info("**** Querying bat_cell_volt_82 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_82"],3)

    return resp.value


def query_bat_cell_volt_83(connection):
    logger.info("**** Querying bat_cell_volt_83 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_83"],3)

    return resp.value


def query_bat_cell_volt_84(connection):
    logger.info("**** Querying bat_cell_volt_84 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_84"],3)

    return resp.value


def query_bat_cell_volt_85(connection):
    logger.info("**** Querying bat_cell_volt_85 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_85"],3)

    return resp.value


def query_bat_cell_volt_86(connection):
    logger.info("**** Querying bat_cell_volt_86 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_86"],3)

    return resp.value


def query_bat_cell_volt_87(connection):
    logger.info("**** Querying bat_cell_volt_87 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_87"],3)

    return resp.value


def query_bat_cell_volt_88(connection):
    logger.info("**** Querying bat_cell_volt_88 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_88"],3)

    return resp.value


def query_bat_cell_volt_89(connection):
    logger.info("**** Querying bat_cell_volt_89 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_89"],3)

    return resp.value


def query_bat_cell_volt_90(connection):
    logger.info("**** Querying bat_cell_volt_90 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_90"],3)

    return resp.value


def query_bat_cell_volt_91(connection):
    logger.info("**** Querying bat_cell_volt_91 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_91"],3)

    return resp.value


def query_bat_cell_volt_92(connection):
    logger.info("**** Querying bat_cell_volt_92 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_92"],3)

    return resp.value


def query_bat_cell_volt_93(connection):
    logger.info("**** Querying bat_cell_volt_93 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_93"],3)

    return resp.value


def query_bat_cell_volt_94(connection):
    logger.info("**** Querying bat_cell_volt_94 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_94"],3)

    return resp.value


def query_bat_cell_volt_95(connection):
    logger.info("**** Querying bat_cell_volt_95 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_95"],3)

    return resp.value


def query_bat_cell_volt_96(connection):
    logger.info("**** Querying bat_cell_volt_96 information ****")
    # Set header to 7E7
    query_command(connection, ext_commands["CAN_HEADER_7E7"])
    # Set the CAN receive address to 7EF
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EF"])

    resp = query_command(connection, ext_commands["BAT_CELL_VOLT_96"],3)

    return resp.value



#########################################################
def query_ac_voltage(connection):
    logger.info("**** Querying battery information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["AC_VOLTAGE"],3)

    return resp.value

def query_ac_current(connection):
    logger.info("**** Querying battery information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["AC_CURRENT"],3)

    return resp.value


def query_elec_coolant_temp(connection):
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["ELEC_COOLANT_TEMP"],3)

    return resp.value

def query_ambient_air_temp(connection):
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["AMBIENT_AIR_TEMP"],3)

    return resp.value


def query_bat_coolant_temp(connection):
    logger.info("**** Querying battery information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_COOLANT_TEMP"],3)

    return resp.value


def query_bat_soc(connection):
    logger.info("**** Querying battery information ****")
    # Set header to 7E4
    query_command(connection, ext_commands["CAN_HEADER_7E4"])
    # Set the CAN receive address to 7EC
    query_command(connection, ext_commands["CAN_RECEIVE_ADDRESS_7EC"])

    resp = query_command(connection, ext_commands["BAT_SOC"],3)

    return resp.value


def publish_data_mqtt(msgs,
                      hostname,
                      port,
                      client_id,
                      user,
                      password,
                      keepalive=60,
                      will=None):
    """Publish all messages to MQTT."""
    try:
        logger.info("Publish messages to MQTT")
        for msg in msgs:
            logger.info("{}".format(msg))

        publish.multiple(msgs,
                         hostname=hostname,
                         port=port,
                         client_id=client_id,
                         keepalive=keepalive,
                         will=will,
                         auth={'username': user, 'password': password},
                         #tls={'tls_version': ssl.PROTOCOL_TLS},
                         #protocol=mqtt.MQTTv311,
                         transport="tcp"
                         )
        logger.info("{} message(s) published to MQTT".format(len(msgs)))
    except Exception as err:
        logger.error("Error publishing to MQTT: {}".format(err), exc_info=False)


def main():
    console_handler = logging.StreamHandler()  # sends output to stderr
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(name)-10s %(levelname)-8s %(message)s"))
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    file_handler = logging.handlers.TimedRotatingFileHandler(os.path.dirname(os.path.realpath(__file__)) + '/../obdii_data.log',
                                                             when='midnight',
                                                             backupCount=15
                                                             )  # sends output to obdii_data.log file rotating it at midnight and storing latest 15 days
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(name)-10s %(levelname)-8s %(message)s"))
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG)

    obd.logger.setLevel(obd.logging.DEBUG)
    # Remove obd logger existing handlers
    for handler in obd.logger.handlers[:]:
        obd.logger.removeHandler(handler)
    # Add handlers to obd logger
    obd.logger.addHandler(console_handler)
    obd.logger.addHandler(file_handler)

    with open(os.path.dirname(os.path.realpath(__file__)) + '/obdii_data.config.json') as config_file:
        config = json.loads(config_file.read())


    broker_address = config['mqtt']['broker']
    port = int(config['mqtt']['port'])
    user = config['mqtt']['user']
    password = config['mqtt']['password']
    topic_prefix = config['mqtt']['topic_prefix']

    print(broker_address,user,topic_prefix) 

    mqtt_msgs = []

    try:
        logger.info("=== Script start ===")

        connection = obd_connect(portstr=config['serial']['port'],
                                 baudrate=int(config['serial']['baudrate']),
                                 fast=False,
                                 timeout=30)

        # Print supported commands
        # DTC = Diagnostic Trouble Codes
        # MIL = Malfunction Indicator Lamp
        logger.debug(connection.print_commands())

        bolt_state={}

        try:
            bolt_state["bat_pack_cap_ah_raw_2018"]=query_bat_pack_cap_ah_raw_2018(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_cap_ah_raw_2018: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_cap_ah_raw_2019"]=query_bat_pack_cap_ah_raw_2019(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_cap_ah_raw_2019: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_cap_kwh_est_2018"]=query_bat_pack_cap_kwh_est_2018(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_cap_kwh_est_2018: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_cap_kwh_est_2019"]=query_bat_pack_cap_kwh_est_2019(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_cap_kwh_est_2019: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_soc_disp"]=query_bat_pack_soc_disp(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_soc_disp: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_soc_raw_hd"]=query_bat_pack_soc_raw_hd(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_soc_raw_hd: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_soc_raw_ld"]=query_bat_pack_soc_raw_ld(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_soc_raw_ld: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_soc_raw_ld2"]=query_bat_pack_soc_raw_ld2(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_soc_raw_ld2: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_soc_raw_ld3"]=query_bat_pack_soc_raw_ld3(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_soc_raw_ld3: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_soc_var"]=query_bat_pack_soc_var(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_soc_var: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_current_hd"]=query_bat_pack_current_hd(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_current_hd: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_num_charges"]=query_bat_pack_num_charges(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_num_charges: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_1"]=query_bat_mod_temp_1(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_1: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_2"]=query_bat_mod_temp_2(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_2: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_3"]=query_bat_mod_temp_3(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_3: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_4"]=query_bat_mod_temp_4(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_4: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_5"]=query_bat_mod_temp_5(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_5: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_6"]=query_bat_mod_temp_6(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_6: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_max"]=query_bat_mod_temp_max(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_max: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_min"]=query_bat_mod_temp_min(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_min: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_mod_temp_avg"]=query_bat_mod_temp_avg(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_mod_temp_avg: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_min"]=query_bat_cell_volt_min(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_min: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_min_num"]=query_bat_cell_volt_min_num(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_min_num: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_max"]=query_bat_cell_volt_max(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_max: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_max_num"]=query_bat_cell_volt_max_num(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_max_num: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_avg"]=query_bat_cell_volt_avg(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_avg: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_resistance"]=query_bat_pack_resistance(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_resistance: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_volt_min"]=query_bat_pack_volt_min(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_volt_min: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_pack_volt_max"]=query_bat_pack_volt_max(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_pack_volt_max: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["hv_current_hd"]=query_hv_current_hd(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying hv_current_hd: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["hv_current"]=query_hv_current(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying hv_current: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["ambient_air_temp"]=query_ambient_air_temp(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying ambient_air_temp: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_01"]=query_bat_cell_volt_01(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_01: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_02"]=query_bat_cell_volt_02(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_02: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_03"]=query_bat_cell_volt_03(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_03: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_04"]=query_bat_cell_volt_04(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_04: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_05"]=query_bat_cell_volt_05(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_05: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_06"]=query_bat_cell_volt_06(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_06: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_07"]=query_bat_cell_volt_07(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_07: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_08"]=query_bat_cell_volt_08(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_08: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_09"]=query_bat_cell_volt_09(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_09: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_10"]=query_bat_cell_volt_10(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_10: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_11"]=query_bat_cell_volt_11(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_11: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_12"]=query_bat_cell_volt_12(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_12: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_13"]=query_bat_cell_volt_13(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_13: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_14"]=query_bat_cell_volt_14(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_14: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_15"]=query_bat_cell_volt_15(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_15: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_16"]=query_bat_cell_volt_16(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_16: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_17"]=query_bat_cell_volt_17(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_17: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_18"]=query_bat_cell_volt_18(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_18: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_19"]=query_bat_cell_volt_19(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_19: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_20"]=query_bat_cell_volt_20(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_20: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_21"]=query_bat_cell_volt_21(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_21: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_22"]=query_bat_cell_volt_22(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_22: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_23"]=query_bat_cell_volt_23(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_23: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_24"]=query_bat_cell_volt_24(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_24: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_25"]=query_bat_cell_volt_25(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_25: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_26"]=query_bat_cell_volt_26(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_26: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_27"]=query_bat_cell_volt_27(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_27: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_28"]=query_bat_cell_volt_28(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_28: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_29"]=query_bat_cell_volt_29(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_29: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_30"]=query_bat_cell_volt_30(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_30: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_31"]=query_bat_cell_volt_31(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_31: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_32"]=query_bat_cell_volt_32(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_32: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_33"]=query_bat_cell_volt_33(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_33: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_34"]=query_bat_cell_volt_34(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_34: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_35"]=query_bat_cell_volt_35(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_35: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_36"]=query_bat_cell_volt_36(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_36: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_37"]=query_bat_cell_volt_37(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_37: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_38"]=query_bat_cell_volt_38(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_38: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_39"]=query_bat_cell_volt_39(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_39: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_40"]=query_bat_cell_volt_40(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_40: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_41"]=query_bat_cell_volt_41(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_41: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_42"]=query_bat_cell_volt_42(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_42: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_43"]=query_bat_cell_volt_43(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_43: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_44"]=query_bat_cell_volt_44(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_44: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_45"]=query_bat_cell_volt_45(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_45: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_46"]=query_bat_cell_volt_46(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_46: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_47"]=query_bat_cell_volt_47(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_47: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_48"]=query_bat_cell_volt_48(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_48: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_49"]=query_bat_cell_volt_49(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_49: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_50"]=query_bat_cell_volt_50(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_50: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_51"]=query_bat_cell_volt_51(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_51: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_52"]=query_bat_cell_volt_52(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_52: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_53"]=query_bat_cell_volt_53(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_53: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_54"]=query_bat_cell_volt_54(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_54: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_55"]=query_bat_cell_volt_55(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_55: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_56"]=query_bat_cell_volt_56(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_56: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_57"]=query_bat_cell_volt_57(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_57: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_58"]=query_bat_cell_volt_58(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_58: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_59"]=query_bat_cell_volt_59(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_59: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_60"]=query_bat_cell_volt_60(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_60: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_61"]=query_bat_cell_volt_61(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_61: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_62"]=query_bat_cell_volt_62(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_62: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_63"]=query_bat_cell_volt_63(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_63: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_64"]=query_bat_cell_volt_64(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_64: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_65"]=query_bat_cell_volt_65(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_65: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_66"]=query_bat_cell_volt_66(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_66: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_67"]=query_bat_cell_volt_67(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_67: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_68"]=query_bat_cell_volt_68(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_68: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_69"]=query_bat_cell_volt_69(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_69: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_70"]=query_bat_cell_volt_70(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_70: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_71"]=query_bat_cell_volt_71(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_71: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_72"]=query_bat_cell_volt_72(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_72: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_73"]=query_bat_cell_volt_73(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_73: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_74"]=query_bat_cell_volt_74(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_74: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_75"]=query_bat_cell_volt_75(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_75: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_76"]=query_bat_cell_volt_76(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_76: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_77"]=query_bat_cell_volt_77(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_77: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_78"]=query_bat_cell_volt_78(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_78: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_79"]=query_bat_cell_volt_79(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_79: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_80"]=query_bat_cell_volt_80(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_80: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_81"]=query_bat_cell_volt_81(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_81: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_82"]=query_bat_cell_volt_82(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_82: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_83"]=query_bat_cell_volt_83(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_83: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_84"]=query_bat_cell_volt_84(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_84: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_85"]=query_bat_cell_volt_85(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_85: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_86"]=query_bat_cell_volt_86(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_86: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_87"]=query_bat_cell_volt_87(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_87: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_88"]=query_bat_cell_volt_88(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_88: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_89"]=query_bat_cell_volt_89(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_89: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_90"]=query_bat_cell_volt_90(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_90: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_91"]=query_bat_cell_volt_91(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_91: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_92"]=query_bat_cell_volt_92(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_92: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_93"]=query_bat_cell_volt_93(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_93: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_94"]=query_bat_cell_volt_94(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_94: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_95"]=query_bat_cell_volt_95(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_95: {} ****".format(err), exc_info=False)
        

        try:
            bolt_state["bat_cell_volt_96"]=query_bat_cell_volt_96(connection)
        except (ValueError, CanError) as err:
            logger.warning("**** Error querying bat_cell_volt_96: {} ****".format(err), exc_info=False)
        

        mqtt_msgs.extend([{'topic': topic_prefix + "state",
                           'payload': json.dumps(bolt_state),
                           'qos': 0,
                           'retain': True}]
                         )

    except OBDIIConnectionError as err:
        logger.error("OBDII connection error: {0}".format(err),
                     exc_info=False)
    except ValueError as err:
        logger.error("Error found: {0}".format(err),
                     exc_info=False)
    except CanError as err:
        logger.error("Error found reading CAN response: {0}".format(err),
                     exc_info=False)
    except Exception as ex:
        logger.error("Unexpected error: {}".format(ex),
                     exc_info=True)

    finally:
        publish_data_mqtt(msgs=mqtt_msgs,
                          hostname=broker_address,
                          port=port,
                          client_id="battery-data-script",
                          user=user,
                          password=password)
        if 'connection' in locals() and connection is not None:
            connection.close()
        logger.info("===  Script end  ===")


if __name__ == '__main__':
    logger = logging.getLogger('obdii')
    main()
