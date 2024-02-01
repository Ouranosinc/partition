""" compute indicators for partition """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
#from dask import config as dskconf
import dask
dask.config.set({'logging.distributed.worker': 'error'})
import xarray as xr
import logging
path = 'config/path_part.yml'
config = 'config/config_part.yml'
from xscen import CONFIG
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)
logger = logging.getLogger('xscen')

if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client', {})

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=3, threads_per_worker=5, memory_limit="8GB",**daskkws):

        dict_sim = xs.search_data_catalogs()

        mod = xs.indicators.load_xclim_module(CONFIG["indicators"]["module"])

        for id, dc in dict_sim.items():

            if not pcat.exists_in_cat(id=id,variable='r20mm',xrfreq='YS-JAN',
                                      processing_level="indicators",):
                logger.info('Computing indicators for %s', id)

                ds_ext = xs.extract_dataset(catalog=dc, )['D']
                ds_ext = ds_ext.chunk(
                    {'rlat': 50, 'rlon': 50} if 'R2' in id else {'lat': 50, 'lon': 50})

                for name, ind in mod.iter_indicators():
                    # Get the frequency and variable names to check if they are already computed
                    outfreq = ind.injected_parameters["freq"].replace('YS','YS-JAN')
                    outnames = [cfatt["var_name"] for cfatt in ind.cf_attrs]
                    if not pcat.exists_in_cat(id=id.replace('CanDCS-U6z','CanDCS-U6'), variable=outnames,xrfreq=outfreq,
                                              processing_level="indicators", ):

                        _, ds_ind = xs.compute_indicators(
                            ds=ds_ext,
                            indicators=[(name, ind)],#"config/indicators-partition.yml",
                        ).popitem()

                        # cheat to bring back name CanDCS-U6
                        # ds_ind.attrs['cat:id'] = ds_ind.attrs['cat:id'].replace('CanDCS-U6z','CanDCS-U6')
                        # ds_ind.attrs['cat:bias_adjust_project'] = ds_ind.attrs[
                        #     'cat:bias_adjust_project'].replace('CanDCS-U6z','CanDCS-U6')

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

                        # fix xrfreq bc weird behavior in between breaking changes in pandas
                        ds_ind.attrs['cat:xrfreq'] = 'YS-JAN'


                        xs.save_and_update(ds=ds_ind,
                                           pcat=pcat,
                                           path=CONFIG['paths']['indicators'],
                                           )


