""" regrid on a regular grid """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
import xarray as xr
import numpy as np
import xesmf
from xscen import CONFIG
import atexit

path = 'config/path_part.yml'
config = 'config/config_part.yml'
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)

# choices
domain = 'QC-reg1c'
ens_name = CONFIG['ens_name']


def add_col(row, d):
    """
    Add a column based on the bias_adjust_project column and a dict of translations.
    """
    if not isinstance(row['bias_adjust_project'], str):
        return np.nan

    for k, v in d.items():
        if k in row['bias_adjust_project']:
            return v


if __name__ == '__main__':
    atexit.register(xs.send_mail_on_exit, subject="Partition regrid")
    daskkws = CONFIG['dask'].get('client', {})
    tdd = CONFIG['tdd']

    # create project catalog
    pcat = xs.ProjectCatalog(CONFIG['project_catalog']['path'],)

    # add reference and adjustment to the catalog
    # this could have been done in indicators.py, but I thought of it too late.
    df = pcat.df.copy()
    pcat.df['adjustment'] = df.apply(add_col, axis=1, d=CONFIG['translate']['adjustment'])
    pcat.df['reference'] = df.apply(add_col, axis=1, d=CONFIG['translate']['reference'])
    pcat.update()

    with Client(n_workers=3, threads_per_worker=5, memory_limit="8GB", **daskkws):

        # create regular grid
        ds_grid = xesmf.util.cf_grid_2d(-83, -55, 0.1, 42, 63, 0.1)
        ds_grid['mask'] = xr.open_zarr(f"{CONFIG['paths']['base']}mask.zarr")['mask']

        # include variable in groupby_attrs
        original_groupby_attributes = pcat.esmcat.aggregation_control.groupby_attrs
        new_groupby_attributes = original_groupby_attributes + ["variable"]
        pcat.esmcat.aggregation_control.groupby_attrs = new_groupby_attributes

        # load input
        dict_input = pcat.search(processing_level="indicators",
                                 domain='QC',
                                 #**CONFIG['ensemble'][ens_name]
                                 ).to_dataset_dict(**tdd)

        for did, ds in dict_input.items():
            var = list(ds.data_vars)[0]
            if not pcat.exists_in_cat(id=did.split('.')[0], domain=domain,
                                      variable=var, processing_level="indicators",):
                print(f'Regrid {domain} {did} {var}')

                out = xs.regrid_dataset(ds=ds[[var]], ds_grid=ds_grid,
                                        to_level=ds.attrs['cat:processing_level'])

                out.attrs['cat:domain'] = domain

                # drop vestigial coords
                out = out.drop_vars('rotated_pole', errors='ignore')

                # for cf
                out.lat.attrs['axis'] = 'Y'
                out.lon.attrs['axis'] = 'X'

                out = out.drop_vars(['lat_bounds', 'lon_bounds'], errors='ignore')

                xs.save_and_update(ds=out,
                                   pcat=pcat,
                                   path=CONFIG['paths']['indicators'],
                                   save_kwargs=dict(
                                       rechunk={'time': -1, 'X': 50, 'Y': 50})
                                   )


