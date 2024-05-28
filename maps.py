""" Create data for maps of uncertainty """
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




if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client', {})

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=2, threads_per_worker=5, memory_limit="30GB",**daskkws):

        # extract
        # dont use to_dataset_dict because with large domain, you need to open from cat
        subcat = pcat.search(processing_level='indicators',
                             domain='QC-reg1',
                             source=CONFIG['source'],
                             reference=CONFIG['reference'])

        # build partition input
        ens_part = xs.ensembles.build_partition_data(
            subcat,
            partition_dim=["source", "experiment", "method", 'reference'],
            to_dataset_kw=CONFIG['tdd'],
            to_level="partition-ensemble84"
        )

        for var in ens_part.data_vars:
            if not pcat.exists_in_cat(processing_level="partition-ensemble84",
                                      variable=var, domain='QC-reg1'):
                print(f"Computing partition-ensemble84 {var}")
                out = ens_part[[var]]
                out.attrs['cat:variable'] = var
                out = out.chunk({'time':-1,'model':-1, 'scenario':-1, 'method':-1, 'reference':-1,
                                 "lat":10, "lon":10})
                xs.save_and_update(out, pcat, CONFIG['paths']['output'])

    with Client(n_workers=2, threads_per_worker=3, memory_limit="30GB", **daskkws):



        for var in ens_part.data_vars:
            if not pcat.exists_in_cat(processing_level="uncertainties84", variable=var,
                                      domain='QC-reg1'):
                print(f"Computing uncertainties84 {var}")
                ens_part = pcat.search(processing_level="partition-ensemble84",
                                                 variable=var,
                                                 domain='QC-reg1').to_dataset(**CONFIG['tdd'])

                #ens_part=ens_part.chunk({'lat':5, 'lon':5})
                print(ens_part)
                mean, uncertainties = xc.ensembles.general_partition(ens_part[var])
                uncertainties = uncertainties.to_dataset(name=var)
                uncertainties.attrs = ens_part.attrs
                uncertainties.attrs['cat:processing_level'] = "uncertainties84"
                uncertainties.attrs['cat:variable'] = var
                print(uncertainties)
                xs.save_and_update(ds=uncertainties,
                                   pcat=pcat,
                                   path=CONFIG['paths']['output'],
                                   )

