import numpy as np
from copy import copy
from datetime import datetime

MY_REF_DATE = datetime.strptime('19700101', '%Y%m%d').date()
MY_VAR = 'days_since_1970'

# gets sorted xarray dataset of multiple bands and returns recent pixel bands with extra band of observed date
# for each pixel

def get_recent_pixel(data_info=None, ref_date=None, my_var=None): # expects dictionary of cell and dataset as key value
    data_dict = {}
    ref_date = ref_date or MY_REF_DATE
    my_var = my_var or MY_VAR
    for k, data in data_info.iteritems():
        bands = len(data.data_vars.keys())
        stored_band = np.zeros((bands+1,4000, 4000), dtype=np.int16)
        dt_list = list()
        dt_tmp_list = data_info[k].time.values.astype('M8[D]').astype('O').tolist()
        print ("looking latest pixels for ", ','.join([dt.strftime('%Y-%m-%d') for dt in dt_tmp_list]))
        for index, dt in enumerate(data_info[k].time.values.astype('M8[D]').astype('O').tolist()):
            ds = data_info[k].isel(time=index)
            days = (dt - ref_date).days
            if np.count_nonzero(stored_band[0]==0) > 0:
                # get latest stored_band data
                latest_pixel_alg(index, days, ds, stored_band)
                dt_list.append(dt)
            else:
                break
        print ("The dates added are ", ','.join([dt.strftime('%Y-%m-%d') for dt in dt_list]))
        print ("time delta for cell " + str(k) + str(len(dt_tmp_list)-len(dt_list)))
        data = data.isel(time=0).drop('time')
        for count in range(bands):
            data_ptr = data.data_vars.values()
            data_ptr[count].data = stored_band[count]
            data_ptr[count].data[data_ptr[count].data==0] = -999
        day_arr = stored_band[bands]
        day_arr[day_arr==0] = -999
        # get exisiting xarray dataset and overwrite the data
        day_data = copy(data.data_vars.values()[0])
        day_data.data = day_arr
        data[my_var] = day_data
        data_dict[k] = data
    return data_dict, dt_list 
 

def latest_pixel_alg(index, days, ds, stored_band):
    # Do update day_arr and individual bands as per latest datasets with its pixel value
    data_con = np.zeros((4000,4000), dtype=np.int16)
    bnds = len(stored_band)
    if index == 0:
        stored_band[bnds-1] = copy(ds.data_vars[ds.data_vars.keys()[0]].values)
        day_arr = stored_band[bnds-1]
        day_arr[day_arr != 0] = days
        for count in range(bnds-1):
            stored_band[count] = copy(ds.data_vars.values()[count].data)
    else:
        day_arr = stored_band[bnds-1]
        data_con = copy(ds.data_vars[ds.data_vars.keys()[0]].values)
        data_con[data_con != 0] = days
        masked_data = day_arr !=0 & np.maximum(day_arr, data_con)
        data_con = np.ma.array(data_con, mask=masked_data)
        data_con = data_con.filled(0)
        stored_band[bnds-1] = day_arr + data_con
        # do for each band now
        for count, band in enumerate(ds.data_vars.items()):
            data_band = ds.data_vars.values()[count].data
            masked_data = stored_band[count] !=0 & np.maximum(stored_band[count], data_band)
            data_band = np.ma.array(data_band, mask=masked_data)
            data_band = data_band.filled(0)
            stored_band[count] = stored_band[count] + data_band
    return 

