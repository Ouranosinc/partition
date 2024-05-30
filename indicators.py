""" compute indicators for partition """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
# from dask import config as dskconf
import dask
dask.config.set({'logging.distributed.worker': 'error'})
import xarray as xr
import logging
path = 'config/path_part.yml'
config = 'config/config_part.yml'
from xscen import CONFIG
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)

if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client', {})

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=3, threads_per_worker=5, memory_limit="8GB", **daskkws):

        dict_sim = xs.search_data_catalogs()

        mod = xs.indicators.load_xclim_module(CONFIG["indicators"]["module"])

        for id, dc in dict_sim.items():

            if not pcat.exists_in_cat(id=id, variable='r20mm', xrfreq='YS-JAN',
                                      processing_level="indicators",):
                print('Computing indicators for ', id)
                chunks = {'rlat': 50, 'rlon': 50, 'time': 1460} if 'R2' in id else {
                    'lat': 50, 'lon': 50, 'time': 1460}

                # PCIC R2 data don't have lat and lon coords. Need to fix it.
                if 'MBCn-R2' in id or 'BCCAQv2-R2' in id:
                    ds_ext = xs.extract_dataset(catalog=dc,
                                                xr_open_kwargs={'chunks': chunks})['D']

                    template=pcat.search(bias_adjust_project='ESPO-G6-R2',
                                         variable='tg_mean', source='MIROC6',
                                         experiment='ssp126', domain='QC').to_dataset()

                    if (template.rlat.drop_vars('rotated_pole').equals(ds_ext.rlat) and
                            template.rlon.drop_vars('rotated_pole').equals(ds_ext.rlon)):
                        ds_ext['rlat'] = template['rlat']
                        ds_ext['rlon'] = template['rlon']
                        ds_ext=ds_ext.assign_coords(
                            {"lon": (('rlat', 'rlon'), template['lon'].data)})
                        ds_ext = ds_ext.assign_coords(
                            {"lat": (('rlat', 'rlon'), template['lat'].data)})
                        ds_ext['lat'].attrs=template['lat'].attrs
                        ds_ext['lon'].attrs = template['lon'].attrs
                    else:
                        print('rlat/rlon of template are not the same as the extracted.')
                else:
                    ds_ext = xs.extract_dataset(catalog=dc,
                                                region=CONFIG['region'],
                                                xr_open_kwargs={'chunks':chunks})['D']

                for name, ind in mod.iter_indicators():
                    # Get the freq and var names to check if they are already computed
                    outfreq = ind.injected_parameters["freq"].replace('YS','YS-JAN')
                    outnames = [cfatt["var_name"] for cfatt in ind.cf_attrs]
                    if not pcat.exists_in_cat(id=id.replace('CanDCS-U6z','CanDCS-U6'),
                                              variable=outnames,xrfreq=outfreq,
                                              processing_level="indicators", ):

                        _, ds_ind = xs.compute_indicators(
                            ds=ds_ext,
                            indicators=[(name, ind)],
                        ).popitem()

                        # add attrs for cf-xarray
                        for c in ['rlon', 'lon']:
                            if c in ds_ind.dims:
                                ds_ind[c].attrs['axis'] = 'X'

                        for c in ['rlat','lat']:
                            if c in ds_ind.dims:
                                ds_ind[c].attrs['axis'] = 'Y'

                        # to avoid issue when saving
                        for c in ds_ind.coords:
                            ds_ind[c].encoding.pop('chunks', None)

                        # put all on same type of time index
                        if isinstance(ds_ind.indexes['time'], xr.CFTimeIndex):
                            ds_ind['time']= ds_ind.indexes['time'].to_datetimeindex()

                        # fix xrfreq bc weird behavior in between changes in pandas
                        ds_ind.attrs['cat:xrfreq'] = 'YS-JAN'

                        xs.save_and_update(ds=ds_ind,
                                           pcat=pcat,
                                           path=CONFIG['paths']['indicators'],
                                           save_kwargs=dict(
                                               rechunk={'time': -1, 'X': 50, 'Y': 50})
                                           )


