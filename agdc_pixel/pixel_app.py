from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import click
# import functool
import sys
import numpy as np
import datetime as DT
import xarray as xr
from datetime import datetime
from itertools import product
import logging
import logging.handlers as lh
import dask.array as da
import datacube
import datacube.api
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


FILE_LOC = '/g/data/u46/users/bxb547/bb'
LOG_FILENAME = FILE_LOC + '/pixel_log_rotating.out'
_log = logging.getLogger('agdc-pixel')
_log.setLevel(logging.INFO)

'''
handler = lh.RotatingFileHandler(LOG_FILENAME,
                                               maxBytes=20000000,
                                               backupCount=5,
                                               )
_log.addHandler(handler)
'''

#: pylint: disable=invalid-name
# required_option = functools.partial(click.option, required=True)
my_data = {}
MY_REF_DATE = datetime.strptime('19700101', '%Y%m%d').date()
PQ_PRODUCTS = ['ls7_pq_albers', 'ls8_pq_albers']
NBAR_PRODUCTS = ['ls7_nbar_albers', 'ls8_nbar_albers']

#: pylint: disable=too-many-arguments
@ui.cli.command(name='latest-pixel')
@ui.global_cli_options
@ui.executor_cli_options
@click.option('--period', 'period', default=200, type=int)
@click.option('--odir', 'odir', default='/g/data/u46/users/bxb547/bb', type=str)
@ui.pass_index(app_name='agdc-pixel-rollover-app')

def main(index, period, odir, executor):
    """
    Uses latest dataset and pixel data to produce netcdf file

    """
    dt1 = datetime.now()
    dt2 = dt1 - DT.timedelta(days=period)
    _log.info('starting pixel_app for last %d days, files output directory %s', period, odir)
    products = ['ls7_nbar_albers', 'ls8_nbar_albers']
    cell_list, my_cell_info = list_all_cells(index, products, period, dt1, dt2)
    tasks = []
    for i, cell in enumerate(cell_list):
        #if i%3 == 0:
        if i > 10:
            break
        pq7 = dict()
        pq8 = dict()
        ls7 = dict()
        ls8 = dict()
        cell = eval(cell)
        if cell in my_cell_info['ls7_pq_albers'] and cell in my_cell_info['ls8_pq_albers']:
            pq7.update(my_cell_info['ls7_pq_albers'][cell])
            pq8.update(my_cell_info['ls8_pq_albers'][cell])
            ls7.update(my_cell_info['ls7_nbar_albers'][cell])
            ls8.update(my_cell_info['ls8_nbar_albers'][cell])
        elif cell in my_cell_info['ls7_pq_albers']:
            pq7.update(my_cell_info['ls7_pq_albers'][cell])
            ls7.update(my_cell_info['ls7_nbar_albers'][cell])
        else:
            pq8.update(my_cell_info['ls8_pq_albers'][cell])
            ls8.update(my_cell_info['ls8_nbar_albers'][cell])
        task = dict(dt1=dt1, dt2=dt2, products=products, period=period,
                    odir=odir, cell=cell, pq7=pq7, pq8=pq8, ls7=ls7, ls8=ls8)
        tasks.append(task)
    results = execute_tasks(executor, index, tasks)
    process_results(executor, results, period, odir)


def process_results(executor, results, period, odir):
    for i, result in enumerate(executor.as_completed(results)):
        dataset = executor.result(result)
        print(dataset)
        create_latest_images(dataset, period, odir) 


def execute_tasks(executor, index, tasks):
    results = []
    # dc = datacube.Datacube(index=index)
    for task in tasks:
        result_future = executor.submit(build_my_dataset, **task)
        results.append(result_future)
    return results


def list_all_cells(index, products, period, dt1, dt2):
    print ("date range is FROM " + str(dt2) + " TO " + str(dt1))
    gw = GridWorkflow(index=index, product=products[0])
    my_cell_info = defaultdict(dict)
    pq_cell_info = defaultdict(dict)
    cell_list = []
    print (" database querying for listing all cells starts at " + str(datetime.now()))
    for prod in PQ_PRODUCTS:
        pq = gw.list_cells(product=prod, time=(dt2, dt1), group_by='solar_day')
        my_cell_info[prod] = pq
        pq_cell_info.update(pq) 
    for prod in NBAR_PRODUCTS:
        data_info = gw.list_cells(product=prod, time=(dt2, dt1), group_by='solar_day')
        my_cell_info[prod] = data_info
    for k,v in pq_cell_info.items():
        cell_list.append(k)
    cell_list = ['({0},{1})'.format(a,b) for (a,b) in cell_list]
    print (" database query done for  all cells " + str(len(cell_list)) + str(datetime.now()))
    return cell_list, my_cell_info


def create_latest_images(data_info, period, odir):
   
    for k,v in data_info.items():
        data = data_info[k] 
        day_arr = np.zeros([data.blue.shape[1], data.blue.shape[2]], dtype=np.int16)
        stored_band = np.zeros((6,4000, 4000), dtype=np.int16)
        dt_list = data_info[k].time.values.astype('M8[D]').astype('O').tolist()
        print ("looking latest pixels for ", ','.join([dt.strftime('%Y-%m-%d') for dt in dt_list]))
        for index, dt in enumerate(data_info[k].time.values.astype('M8[D]').astype('O').tolist()):
            ds = data_info[k].isel(time=index)
            days = (dt - MY_REF_DATE).days
            day_arr = update_latest_pixel(index, days, ds, day_arr, stored_band)
        data = data.isel(time=0).drop('time')
        for count, band in enumerate([data.blue, data.green, data.red, data.nir, data.swir1, data.swir2]):
            band.data = stored_band[count]
            band.data[band.data==0] = -999 
        day_arr[day_arr==0] = -999 
        day_arr = xr.DataArray(day_arr, coords=data.coords, dims=['y', 'x'])
        data['days_since_1970'] = day_arr
        my_data[k] = data
        if odir:
            global_attributes = {} 
            global_attributes = dict(Comment1 = 'Data acquired on ' 
                                     + ','.join([dt.strftime('%Y-%m-%d') for dt in dt_list]))
            FILE_LOC = odir
            filename = FILE_LOC + '/' + 'LATEST_PIXEL_' + ''.join(map(str, k)) + "_" + str(datetime.now().date()
                        ) + "_CLOUD_FREE_LAST_" + str(period) + "_DAYS.nc"
            try:
                write_dataset_to_netcdf(data, global_attributes=global_attributes,
                                        variable_params={'blue': {'zlib':True}, 
                                                         'green': {'zlib':True}, 'red': {'zlib':True},
                                                         'nir': {'zlib':True}, 'swir1': {'zlib':True},
                                                         'swir2': {'zlib':True},
                                                         'days_since_1970': {'zlib':True}},
                                                         filename=Path(filename))
            except RuntimeError as e:
                print (e)
                return
            print ("Written onto " + filename) 
            _log.info('Data written onto %s', filename)
        else:
            print ("computing finished and ready as dictionary in my_data ", str(datetime.now()))
       
def update_latest_pixel(index, days, ds, day_arr, stored_band):
    # Do update day_arr and individual bands as per latest datasets with its pixel value
    data_con = np.zeros((4000,4000), dtype=np.int16)
    if index == 0:
        day_arr =  copy(ds.blue.values)
        day_arr[day_arr != 0] = days
        for count, band in enumerate([ds.blue, ds.green, ds.red, ds.nir, ds.swir1, ds.swir2]):
            stored_band[count] = band.values
    else:
        data_con = copy(ds.blue.values) 
        data_con[data_con != 0] = days
        masked_data = day_arr !=0 & np.maximum(day_arr, data_con)
        data_con = np.ma.array(data_con, mask=masked_data) 
        data_con = data_con.filled(0)
        day_arr = day_arr + data_con
        # do for each band now
        for count, band in enumerate([ds.blue, ds.green, ds.red, ds.nir, ds.swir1, ds.swir2]):
            data_band = band.values
            masked_data = stored_band[count] !=0 & np.maximum(stored_band[count], data_band)
            data_band = np.ma.array(data_band, mask=masked_data)
            data_band = data_band.filled(0)
            stored_band[count] = stored_band[count] + data_band
    return day_arr

def rollover_task(index, cell, products, period, odir):
    dt1 = datetime.now()
    dt2 = dt1 - DT.timedelta(days=period)
    print ("date range is FROM " + str(dt2) + " TO " + str(dt1))
    # gather latest datasets as per product names and build a dataset dictionary of 
    # unique cells/datasets against the latest dates
    data_info = build_my_dataset(index, dt1, dt2, products, period, odir, cell)
    create_latest_images(data_info, period, odir)

def pq_fuser(dest, src):
    valid_bit = 8
    valid_val = (1 << valid_bit)
    no_data_dest_mask = ~(dest & valid_val).astype(bool)
    np.copyto(dest, src, where=no_data_dest_mask)
    both_data_mask = (valid_val & dest & src).astype(bool)
    np.copyto(dest, src & dest, where=both_data_mask)


def build_my_dataset(dt1, dt2, products, period, odir, cell=None, **prod):
    ls_7 = defaultdict()
    ls_8 = defaultdict()
    print (" building dataset for my cell ", cell )
    # pq = GridWorkflow.list_cells(eval(cl), product=prod, time=(dt2, dt1), group_by='solar_day')
    print (" loading data at ", str(datetime.now().time()))
    if prod['pq7']:
        data = GridWorkflow.load(prod['ls7'], measurements=['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])
        print (" loaded nbar data for LS7" , str(datetime.now().time())) 
        pq = GridWorkflow.load(prod['pq7'], fuse_func=pq_fuser)
        print (" loaded pq data for sensor LS7 ", str(datetime.now().time())) 
        mask_clear = pq['pixelquality'] & 15871 == 15871
        ndata = data.where(mask_clear).astype(np.int16)
    # sort in such that latest date comes first
        ndata = ndata.sel(time=sorted(ndata.time.values, reverse=True))
        if len(ndata.attrs) == 0:
            ndata.attrs = data.attrs
        ls_7[cell] = copy(ndata)
    if prod['pq8']:
        data = GridWorkflow.load(prod['ls8'], measurements=['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])
        print (" loaded nbar data for LS8", str(datetime.now().time())) 
        pq = GridWorkflow.load(prod['pq8'], fuse_func=pq_fuser)
        print (" loaded pq data for LS8 ", str(datetime.now().time())) 
        mask_clear = pq['pixelquality'] & 15871 == 15871
        ndata = data.where(mask_clear).astype(np.int16)
    # sort in such that latest date comes first
        ndata = ndata.sel(time=sorted(ndata.time.values, reverse=True))
        if len(ndata.attrs) == 0:
            ndata.attrs = data.attrs
        ls_8[cell] = copy(ndata)
                
    my_set = set()
    for k, v in ls_8.items():
        my_set.add(k)
    for k, v in ls_7.items():
        my_set.add(k)
    ls_new={}
    for k in list(my_set):
        if k in ls_8 and k in ls_7:
            ls_new[k] = xr.concat([ls_8[k], ls_7[k]], dim='time')
            ls_new[k] = ls_new[k].sel(time=sorted(ls_new[k].time.values, reverse=True))
        elif k in ls_7:
            ls_new[k] = ls_7[k] 
        elif k in ls_8:
            ls_new[k] = ls_8[k] 
    return ls_new

if __name__ == '__main__':
    '''
    The program gets latest LANDSAT 7/8 dataset and creates netcdf file or output as in dictionary after 
    applying cloud free pq data. If no output directory is mentioned then it puts cell as key and seven bands
    dataset as value. The program strictly checks pq data. If it is not available then do not process further.
    
    dc = datacube.Datacube(app="pixel-rollover")
    products = ['ls7_nbar_albers', 'ls8_nbar_albers']
    cell = '(12,-38)'
    period = 90
    odir = None
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
            print("my inputs period and cells ", period, cell)
            _log.info("my inputs period %d cells %s" % (period, cell)) 
        else:
            print("my inputs are period" + period + " cells " + cells + " odir loc " + odir )
            _log.info("my inputs are period %d, cells and %s odir %s" % (period, cell, odir)) 
    else:
        print ("Please pass period and cell like  '100/(12,-40)/(13,-39)' to get a dictionary of")
        print (" my_data to play in jupyter. OR Please pass period, cell and output dir like")
        print (" '100/(12,-40)/(13,-39)/ODIR=/g/data/u46/users/blah'")
        print (" to get netcdf file in your choice of location")
        exit(0)
    rollover_task(dc.index, cell, products, period, odir)
    '''
    main()

