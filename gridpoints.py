""" Create data for uncertainty plots at given gridpoints """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
import dask
dask.config.set({'logging.distributed.worker': 'error'})
import xclim as xc
import logging
path = 'config/path_part.yml'
config = 'config/config_part.yml'
from xscen import CONFIG
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)
logger = logging.getLogger('xscen')

# choices
ens_name = '84'
domain = 'QC'

if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client', {})

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=2, threads_per_worker=5, memory_limit="30GB", **daskkws):
        level_un = f"uncertainties{ens_name}"
        # extract QC
        datasets = pcat.search(processing_level='indicators',
                               domain=domain,
                               **CONFIG['ensemble'][ens_name]
                               ).to_dataset_dict(**CONFIG['tdd'])

        for point_name, point in CONFIG['gridpoints'].items():

            # build partition input
            ens_part = xs.ensembles.build_partition_data(
                datasets,
                partition_dim=["source", "experiment", "method", 'reference'],
                subset_kw=point,
            )

            for i, var in enumerate(ens_part.data_vars):
                if not pcat.exists_in_cat(processing_level= level_un,
                                          variable=var, domain=point_name):

                    #compute uncertainties per category
                    mean, uncertainties = xc.ensembles.general_partition(ens_part[var])
                    uncertainties = uncertainties.to_dataset(name=var)
                    uncertainties.attrs = ens_part.attrs
                    uncertainties.attrs['cat:processing_level'] = level_un
                    uncertainties.attrs['cat:variable'] = var
                    # save
                    xs.save_and_update(ds=uncertainties,
                                       pcat=pcat,
                                       path=CONFIG['paths']['output'],
                                       )
