""" Regrid once on each reference, find gridpoints with nans in any reference
 to build the mask. """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
import dask
import xarray as xr
import xesmf
import logging
import atexit
from xscen import CONFIG

path = 'config/path_part.yml'
config = 'config/config_part.yml'
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)
logger = logging.getLogger('xscen')
dask.config.set({'logging.distributed.worker': 'error'})


if __name__ == '__main__':
    atexit.register(xs.send_mail_on_exit, subject=CONFIG['scripting']['subject'])
    daskkws = CONFIG['dask'].get('client', {})
    tdd = CONFIG['tdd']

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=3, threads_per_worker=5, memory_limit="8GB", **daskkws):

        # create regular grid
        ds_grid = xesmf.util.cf_grid_2d(-83, -55, 0.1, 42, 63, 0.1)

        # load input, on per ref
        dict_input = pcat.search(processing_level="indicators",
                                 method='IC6',
                                 source='CanESM5',
                                 experiment='ssp245',
                                 variable='tg_mean',
                                 domain='QC').to_dataset_dict(**tdd)

        for did, ds in dict_input.items():
            var = list(ds.data_vars)[0]
            if not pcat.exists_in_cat(id=did.split('.')[0], domain='QC-reg1c-mask',
                                      variable=var, processing_level="indicators",):
                logger.info(f'Regrid {did} {var} to find mask.')

                out = xs.regrid_dataset( ds=ds[[var]], ds_grid=ds_grid,
                                         to_level=ds.attrs['cat:processing_level'])

                out.attrs['cat:domain'] = 'QC-reg1c-mask'

                # drop vestigial coords
                out = out.drop_vars('rotated_pole', errors='ignore')

                # for cf
                out.lat.attrs['axis'] = 'Y'
                out.lon.attrs['axis'] = 'X'

                xs.save_and_update(ds=out,
                                   pcat=pcat,
                                   path=CONFIG['paths']['indicators'],
                                   save_kwargs=dict(
                                       rechunk={'time': -1, 'X': 50, 'Y': 50})
                                   )

        # build mask
        # if any nan in any ref, mask
        ds = pcat.search(
            processing_level="indicators",
            domain='QC-reg1c-mask').to_dataset(concat_on='bias_adjust_project')

        count_nans = xr.where(ds.tg_mean.isel(time=0).isnull(), 0, 1
                              ).mean(dim='bias_adjust_project').drop_vars('time')
        mask = xr.where(count_nans == 1, 1, 0).to_dataset(name='mask')
        xs.save_to_zarr(mask, f"{CONFIG['paths']['base']}mask.zarr",
                        mode='o')
