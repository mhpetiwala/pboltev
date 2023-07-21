from obd.utils import bytes_to_int
from utils import bytes_to_int_signed


def bat_pack_cap_ah_raw_2018(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)/10
    return ((d[3]*256)+d[4])/10

def bat_pack_cap_ah_raw_2019(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)/100
    return ((d[3]*256)+d[4])/100

def bat_pack_cap_ah_est_2018(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)*0.032
    return ((d[3]*256)+d[4])*0.032

def bat_pack_cap_ah_est_2019(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)*0.0032
    return ((d[3]*256)+d[4])*0.0032

def bat_pack_soc_disp(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #A*100/255
    return d[3]*100/255

def bat_pack_soc_raw_hd(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((((A*256)+B)*100)/65535)
    return ((((d[3]*256)+d[4])*100)/65535)

def bat_pack_soc_raw_ld_var(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #A/2.55
    return d[3]/2.55

def bat_pack_current_hd(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((Signed(A)*256)+B)/20
    return ((bytes_to_int_signed(d[3])*256)+d[4])/20

def bat_pack_num_charges(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A<8)+B)
    return ((d[3]<8)+d[4])

def bat_mod_temp(messages): 
    d=messages[0].data
    if len(d) == 0:
        return None
    #A-40
    return d[3]-40

def bat_cell_volt_min_max(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)/1666.666
    return ((d[3]*256)+d[4])/1666.666

def bat_cell_volt_min_max_num(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #A
    return d[3]

def bat_cell_volt_avg(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)*5/65535
    return ((d[3]*256)+d[4])*5/65535

def bat_pack_resistance(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)/2
    return ((d[3]*256)+d[4])/2

def bat_pack_volt_min_max(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)*0.52
    return ((d[3]*256)+d[4])*0.52

def hv_current_hd(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #(Signed(A)*256+B)/20
    return (bytes_to_int_signed(d[3])*256+d[4])/20

def hv_current(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #(Signed(A)*256+B)/(-6.675)
    return (bytes_to_int_signed(d[3])*256+d[4])/(-6.675)

def ambient_air_temp(messages): 
    d=messages[0].data
    if len(d) == 0:
        return None
    #(A-40)
    return d[3]-40

def bat_cell_volt(messages):
    d=messages[0].data
    if len(d) == 0:
        return None
    #((A*256)+B)*5/65535
    return ((d[3]*256)+d[3])*5/65535

