""" regrid on a regular grid """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
import dask
dask.config.set({'logging.distributed.worker': 'error'})
import xarray as xr
import xesmf
import logging
path = 'config/path_part.yml'
config = 'config/config_part.yml'
from xscen import CONFIG
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)
logger = logging.getLogger('xscen')

if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client', {})
    tdd = CONFIG['tdd']

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=3, threads_per_worker=5, memory_limit="8GB",**daskkws):

        # create regular grid
        ds_grid = xesmf.util.cf_grid_2d(-83, -55, 0.1, 42, 63, 0.1)

        # include variable in groupby_attrs
        original_groupby_attributes = pcat.esmcat.aggregation_control.groupby_attrs
        new_groupby_attributes = original_groupby_attributes + ["variable"]
        pcat.esmcat.aggregation_control.groupby_attrs = new_groupby_attributes

        # load input
        dict_input = pcat.search(processing_level="indicators",
                                 domain='QC').to_dataset_dict(**tdd)


        for id, ds in dict_input.items():
            var = list(ds.data_vars)[0]
            if not pcat.exists_in_cat(id=id.split('.')[0],domain='QC-reg1',
                                      variable=var, processing_level="indicators",):
                logger.info(f'Regrid {id} {var}')

                out = xs.regrid_dataset( ds=ds[[var]], ds_grid= ds_grid,
                                         to_level=ds.attrs['cat:processing_level']  )

                out.attrs['cat:domain'] = 'QC-reg1'

                # for cf
                out.lat.attrs['axis'] = 'Y'
                out.lon.attrs['axis'] = 'X'

                xs.save_and_update(ds=out,
                                   pcat=pcat,
                                   path=CONFIG['paths']['indicators'],
                                   )


        # add reference and method to the catalog
        def add_method(row):
            if 'ESPO-G6' in row['bias_adjust_project']:
                return 'ESPO-G6'
            elif 'IC6' in row['bias_adjust_project']:
                return 'IC6'
            elif 'CanDCS-U6' in row['bias_adjust_project']:
                return 'BCCAQv2'
            elif 'CanDCS-M6' in row['bias_adjust_project']:
                return 'MBCn'
            elif 'NEX' in row['bias_adjust_project']:
                return 'NEX-GDDP'
            elif 'MBCn' in row['bias_adjust_project']:
                return 'MBCn'
            elif 'BCCAQv2' in row['bias_adjust_project']:
                return 'BCCAQv2'


        def add_reference(row):
            if 'R2' in row['bias_adjust_project']:
                return 'CaSRv2.1'
            elif 'E5L' in row['bias_adjust_project']:
                return 'ERA5-Land'
            elif 'EM' in row['bias_adjust_project']:
                return 'EMDNA'
            elif 'CanDCS-M6' in row['bias_adjust_project']:
                return 'PCICBlend'
            elif 'PB' in row['bias_adjust_project']:
                return 'PCICBlend'
            elif 'CanDCS-U6' in row['bias_adjust_project']:
                return 'NRCANmet'
            elif 'N2014' in row['bias_adjust_project']:
                return 'NRCANmet'
            elif 'NEX' in row['bias_adjust_project']:
                return 'GMFD'


        df = pcat.df.copy()
        pcat.df['method'] = df.apply(add_method, axis=1)
        pcat.df['reference'] = df.apply(add_reference, axis=1)
        pcat.update()