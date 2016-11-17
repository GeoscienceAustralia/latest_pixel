from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import click
# import functool
import sys
import os
import numpy as np
import rasterio
import datetime as DT
import xarray as xr
from datetime import datetime
from itertools import product
import logging
import logging.handlers as lh
import dask.array as da
import datacube.api
import fnmatch
from datacube.ui.expression import parse_expressions
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from datacube.api.geo_xarray import append_solar_day
from datacube.storage.masking import make_mask
from dateutil.rrule import rrule, YEARLY
from datacube.ui import click as ui
from enum import Enum
from datacube.api import GridWorkflow
from copy import copy
from pathlib import Path
from datacube.storage.storage import write_dataset_to_netcdf
from rasterio.enums import ColorInterp
from gqa_filter import list_gqa_filtered_cells, get_gqa
from recent_pixel_util import get_recent_pixel

logging.basicConfig()
_log = logging.getLogger('agdc-pixel-test')
_log.setLevel(logging.INFO)
#: pylint: disable=invalid-name
# required_option = functools.partial(click.option, required=True)
my_data = {}
MY_REF_DATE = datetime.strptime('19700101', '%Y%m%d').date()
MY_OBS_VAR = 'days_since_1970'
DEFAULT_PROFILE = {
    'blockxsize': 256,
    'blockysize': 256,
    'compress': 'lzw',
    'driver': 'GTiff',
    'interleave': 'band',
    'nodata': -999,
    'tiled': True}


def main(index, products, expressions, executor):
    """
    Uses latest dataset and pixel data to produce netcdf file

    """
    tasks = create_pixel_tasks(products, expressions)

    results = execute_tasks(executor, index, tasks)

    process_results(executor, results)


def create_files(data_ret, odir, MY_OBS_VAR, dt_list):
    for k,data in data_ret.items():
        if len(odir) > 0:
            global_attributes = {}
            global_attributes = dict(Comment1 = 'Data observed on '
                                     + ','.join([dt.strftime('%Y-%m-%d') for dt in dt_list]))
            filename = odir + '/' + 'LATEST_PIXEL_' + ''.join(map(str, k)) \
                       + "_CLOUD_FREE_LAST_" + str(period) + "_DAYS"
            obs_filename = odir + '/' + 'LATEST_PIXEL_' + ''.join(map(str, k)) \
                       + "_CLOUD_FREE_LAST_" + str(period) + "_DAYS_OBS"

            try:
                ncfl = filename + ".nc"
                ncobs = obs_filename + ".nc"
                filename = filename + ".tif"
                obs_filename = obs_filename + ".tif"
                write_dataset_to_netcdf(data[[MY_OBS_VAR]], global_attributes=global_attributes,
                                        variable_params={MY_OBS_VAR: {'zlib':True}},
                                                         filename=Path(ncobs))
                write_dataset_to_netcdf(data[['swir1', 'nir', 'green']], global_attributes=global_attributes,
                                        variable_params={'swir1': {'zlib':True},
                                                         'nir': {'zlib':True},
                                                         'green': {'zlib':True}},
                                                         filename=Path(ncfl))
                write_geotiff(filename=obs_filename, dataset=data[[MY_OBS_VAR]])
                write_geotiff(filename=filename, dataset=data[['swir1', 'nir', 'green']],
                              profile_override={'photometric':'RGB'})
            except RuntimeError as e:
                _log.info('File exists ', e)
                return
        else:
            # data['days_since_1970'] = day_arr
            my_data[k] = data
            print ("computing finished and ready as dictionary in my_data ", str(datetime.now()))

def create_latest_images(data_info, duration, odir):
   
    for k,v in data_info.iteritems():
        data = data_info[k] 
        day_arr = np.zeros([data.swir1.shape[1], data.swir1.shape[2]], dtype=np.int16)
        stored_band = np.zeros((6,4000, 4000), dtype=np.int16)
        dt_list = list()
        dt_tmp_list = data_info[k].time.values.astype('M8[D]').astype('O').tolist()
        print ("looking latest pixels for ", ','.join([dt.strftime('%Y-%m-%d') for dt in dt_tmp_list]))
        for index, dt in enumerate(data_info[k].time.values.astype('M8[D]').astype('O').tolist()):
           
            ds = data_info[k].isel(time=index)
            days = (dt - MY_REF_DATE).days
            print ("count of zero pixel",  str(np.count_nonzero(stored_band[0]==0)))
            if np.count_nonzero(stored_band[0]==0) > 0:
                day_arr = update_latest_pixel(index, days, ds, day_arr, stored_band)
                dt_list.append(dt)
            else:
                break
        print ("The dates added are ", ','.join([dt.strftime('%Y-%m-%d') for dt in dt_list]))
        print ("time delta for cell " + str(k) + str(len(dt_tmp_list)-len(dt_list))) 
        data = data.isel(time=0).drop('time')
        for count, band in enumerate([data.swir1, data.nir, data.green]):
            band.data = stored_band[count]
            band.data[band.data==0] = -999 
        day_arr[day_arr==0] = -999 
        day_arr = xr.DataArray(day_arr, coords=data.coords, dims=['y', 'x'])
        my_data[k] = data
        if len(odir) > 0:
            day_ds = day_arr.to_dataset(name='days_since_1970')
            day_ds.attrs = data.attrs
            global_attributes = {} 
            global_attributes = dict(Comment1 = 'Data observed on ' 
                                     + ','.join([dt.strftime('%Y-%m-%d') for dt in dt_list]))
            filename = odir + '/' + 'LATEST_PIXEL_' + ''.join(map(str, k)) \
                       + "_CLOUD_FREE_LAST_" + str(period) + "_DAYS"
            obs_filename = odir + '/' + 'LATEST_PIXEL_' + ''.join(map(str, k)) \
                       + "_CLOUD_FREE_LAST_" + str(period) + "_DAYS_OBS"

            try:
                ncfl = filename + ".nc"
                ncobs = obs_filename + ".nc"
                write_dataset_to_netcdf(data, global_attributes=global_attributes,
                                        variable_params={'swir1': {'zlib':True},
                                                         'nir': {'zlib':True},
                                                         'green': {'zlib':True}},
                                                         filename=Path(ncfl))
                write_dataset_to_netcdf(day_ds, global_attributes=global_attributes,
                                        variable_params={'days_since_1970': {'zlib':True}},
                                                         filename=Path(ncobs))
                filename = filename + ".tif"
                obs_filename = obs_filename + ".tif"
                write_geotiff(filename=filename, dataset=data, 
                              profile_override={'photometric':'RGB'})
                write_geotiff(filename=obs_filename, dataset=day_ds)
            except RuntimeError as e:
                _log.info('File exists ', e)
                return
        else:
            data['days_since_1970'] = day_arr
            my_data[k] = data
            print ("computing finished and ready as dictionary in my_data ", str(datetime.now()))
       
def update_latest_pixel(index, days, ds, day_arr, stored_band):
    # Do update day_arr and individual bands as per latest datasets with its pixel value
    data_con = np.zeros((4000,4000), dtype=np.int16)
    if index == 0:
        day_arr =  copy(ds.swir1.values)
        day_arr[day_arr != 0] = days
        for count, band in enumerate([ds.swir1, ds.nir, ds.green]):
            stored_band[count] = band.values
    else:
        data_con = copy(ds.swir1.values) 
        data_con[data_con != 0] = days
        masked_data = day_arr !=0 & np.maximum(day_arr, data_con)
        data_con = np.ma.array(data_con, mask=masked_data) 
        data_con = data_con.filled(0)
        day_arr = day_arr + data_con
        # do for each band now
        for count, band in enumerate([ds.swir1, ds.nir, ds.green]):
            data_band = band.values
            masked_data = stored_band[count] !=0 & np.maximum(stored_band[count], data_band)
            data_band = np.ma.array(data_band, mask=masked_data)
            data_band = data_band.filled(0)
            stored_band[count] = stored_band[count] + data_band
    return day_arr

def rollover_task(index, cell, products, duration, odir):
    dt1 = datetime.now()
    dt2 = dt1 - DT.timedelta(days=duration)
    print ("date range is FROM " + str(dt2) + " TO " + str(dt1))
    # check first whether file name exists
    if len(odir) > 0:
        for cl in  cell:
            filename = 'LATEST_PIXEL_' + ''.join(map(str, eval(cl)))  \
                   + "_CLOUD_FREE_LAST_*"
            print ("my cell and filename  ", cl, filename )
            for file in os.listdir(odir):
                if fnmatch.fnmatch(file, filename):
                    print ("file exists " + filename)
                    return
    # gather latest datasets as per product names and build a dataset dictionary of 
    # unique cells/datasets against the latest dates
   
    data_info = build_my_dataset(index, dt1, dt2, products, cell)
    # create_pixels_tasks(products, expressions)
    data_ret, dt_list = get_recent_pixel(data_info, MY_REF_DATE, MY_OBS_VAR)
    #create_latest_images(data_info, duration, odir)
    create_files(data_ret, odir, MY_OBS_VAR, dt_list)

def pq_fuser(dest, src):
    valid_bit = 8
    valid_val = (1 << valid_bit)
    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)
    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)


def build_my_dataset(index, dt1, dt2, products, cell=None):
    gw = GridWorkflow(index=index, product=products[0])
    ls_7 = defaultdict()
    ls_8 = defaultdict()
    new_ds = np.empty(1, dtype=object)
    for st in products:
        pq_cell_info = defaultdict(dict)
        cell_info = defaultdict(dict)
        prod = None        
        if st == 'ls7_nbar_albers':
            prod = 'ls7_pq_albers'
        else:
            prod = 'ls8_pq_albers'
        for cl in  cell:
            print ("my cell and sensor", cl, st )
            filepath = odir + '/' + 'LATEST_PIXEL_' + ''.join(map(str, eval(cl)))  \
                   + "_CLOUD_FREE_LAST_" + str(period) + "_DAYS.nc"
            if os.path.isfile(filepath):
                print ("file exists " + filepath)
                continue
            #pq = gw.list_cells(eval(cl), product=prod, time=(dt2, dt1), group_by='solar_day')
            indexers = {'product':prod, 'time':(dt2, dt1), 'group_by':'solar_day'}
            pq = list_gqa_filtered_cells(index, gw, pix_th=1, cell_index=eval(cl), **indexers)
            if len(pq) == 0:
                _log.info("NO PQ INFO FOUND FOR CELL %s AND IGNORING FOR THIS SENSOR %s" % (cl, st))
                print ("NO PQ DATA FOUND AND IGNORING FOR THIS SENSOR ", cl, st)
                continue
            pq_cell_info.update(pq)
        for cl, vl in pq_cell_info.items():
            cell_info.update(gw.list_cells(cl, product=st, time=(dt2, dt1), group_by='solar_day'))
        for k, v in cell_info.iteritems(): 
            if type(k) == tuple:
                print (" loading data for sensor at ", st, str(datetime.now().time())) 
                data = gw.load(cell_info[k], measurements=['swir1', 'nir', 'green'])
                print (" loaded nbar data" , str(datetime.now().time())) 
                pq = gw.load(pq_cell_info[k], fuse_func=pq_fuser)
                print (" loaded pq data for sensor at ", st, str(datetime.now().time())) 
                mask_clear = pq['pixelquality'] & 15871 == 15871
                ndata = data.where(mask_clear).astype(np.int16)
                # sort in such that latest date comes first
                ndata = ndata.sel(time=sorted(ndata.time.values, reverse=True))
                if len(ndata.attrs) == 0:
                    ndata.attrs = data.attrs
                if st == 'ls7_nbar_albers':
                    ls_7[k] = copy(ndata)
                else:
                    ls_8[k] = copy(ndata)
    my_set = set()
    for k, v in ls_8.items():
        my_set.add(k)
    for k, v in ls_7.items():
        my_set.add(k)
    ls_new={}
    for k in list(my_set):
        if k in ls_8 and k in ls_7:
            ls_new[k] = xr.concat([ls_8[k], ls_7[k]], dim='time')
            ls_new[k] = ls_new[k][['swir1', 'nir', 'green']]
            ls_new[k] = ls_new[k].sel(time=sorted(ls_new[k].time.values, reverse=True))
        elif k in ls_7:
            ls_new[k] = ls_7[k] 
        elif k in ls_8:
            ls_new[k] = ls_8[k] 
    return ls_new
    

def create_pixel_tasks(products):
    tasks = []
    start_date, end_date = get_start_end_dates(expressions)
    for acq_min, acq_max in get_epochs(epoch, start_date, end_date):
        task = dict(products=products, acq_range=(acq_min, acq_max), season=season,
                    measurements=measurements,
                    stats=stats, masks=masks, epoch=epoch, expressions=expressions,
                    computed_measurements=computed_measurements)
        tasks.append(task)
    return tasks


def get_start_end_dates(expressions):
    parsed = parse_expressions(*expressions)
    time_range = parsed['time']
    return time_range.begin, time_range.end


def write_geotiff(filename, dataset, time_index=None, profile_override=None):
    """
    Write an xarray dataset to a geotiff

    :attr bands: ordered list of dataset names
    :attr time_index: time index to write to file
    :attr dataset: xarray dataset containing multiple bands to write to file
    :attr profile_override: option dict, overrides rasterio file creation options.
    """
    profile_override = profile_override or {}

    dtypes = {val.dtype for val in dataset.data_vars.values()}
    assert len(dtypes) == 1  # Check for multiple dtypes
    profile = DEFAULT_PROFILE.copy()
    profile.update({
        'width': dataset.dims[dataset.crs.dimensions[1]],
        'height': dataset.dims[dataset.crs.dimensions[0]],
        'affine': dataset.affine,
        'crs': dataset.crs.crs_str,
        'count': len(dataset.data_vars),
        'dtype': str(dtypes.pop())
    })
    profile.update(profile_override)

    with rasterio.open(filename, 'w', **profile) as dest:
        for bandnum, data in enumerate(dataset.data_vars.values(), start=1):
            dest.write(data.data, bandnum)


if __name__ == '__main__':
    '''
    The program gets latest LANDSAT 7/8 dataset and creates netcdf file or output as in dictionary after 
    applying cloud free pq data. If no output directory is mentioned then it puts cell as key and seven bands
    dataset as value. The program strictly checks pq data. If it is not available then do not process further.
    '''
    dc = datacube.Datacube(app="pixel-rollover")
    products = ['ls7_nbar_albers', 'ls8_nbar_albers']
    cell = '(12,-38)'
    period = 90
    odir = '' 
    if len(sys.argv) > 1:
        my_args = sys.argv[1:]
        my_args = ''.join(my_args)
        if len(my_args.split('/ODIR=')) > 1:
           odir = my_args.split('/ODIR=')[1]
           my_args =  my_args.split('/ODIR=')[0]
        if int(my_args.split('/')[0]) > 0:
            period = int(my_args.split('/')[0])
        if my_args.split('/')[1:]:
            cell = my_args.split('/')[1:] 
        if odir is None:
            _log.info("my inputs period %d cells %s" % (period, cell)) 
        else:
            _log.info("my inputs are period %d, cells and %s odir %s" % (period, cell, odir)) 
    else:
        print ("Please pass period and cell like  '100/(12,-40)/(13,-39)' to get a dictionary of")
        print (" my_data to play in jupyter. OR Please pass period, cell and output dir like")
        print (" '100/(12,-40)/(13,-39)/ODIR=/g/data/u46/users/blah'")
        print (" to get netcdf file in your choice of location")
        exit(0)
    rollover_task(dc.index, cell, products, period, odir)

