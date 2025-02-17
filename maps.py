""" Create data for maps of uncertainty """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
import atexit
import xclim as xc
from xscen import CONFIG

path = 'config/path_part.yml'
config = 'config/config_part.yml'
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)

# choices
ens_name = CONFIG['ens_name']
domain = 'QC-reg1c'


if __name__ == '__main__':
    atexit.register(xs.send_mail_on_exit, subject="Partition maps")
    daskkws = CONFIG['dask'].get('client', {})

    # create project catalog
    pcat = xs.ProjectCatalog(CONFIG['project_catalog']['path'],)

    with Client(n_workers=2, threads_per_worker=5, memory_limit="30GB", **daskkws):
        level_part = f"partition-ensemble{ens_name}"

        # extract
        # don't use to_dataset_dict because with large domain, you need to open from cat
        subcat = pcat.search(processing_level='indicators',
                             domain=domain,
                             **CONFIG['ensemble'][ens_name])


        # build partition input
        ens_part = xs.ensembles.build_partition_data(
            subcat,
            partition_dim=["source", "experiment", "adjustment", 'reference'],
            to_dataset_kw=CONFIG['tdd'],
            to_level=level_part
        )

        for var in ens_part.data_vars:
            if not pcat.exists_in_cat(processing_level=level_part,
                                      variable=var, domain=domain):
                print(f"Computing {level_part} {var}")
                out = ens_part[[var]]
                out.attrs['cat:variable'] = var
                out = out.chunk({'time': -1, 'model': -1, 'scenario': -1, "lon": 10,
                                 'adjustment': -1, 'reference': -1, "lat": 10})
                xs.save_and_update(out, pcat, CONFIG['paths']['output'])

    # compute uncertainties
    with Client(n_workers=2, threads_per_worker=5, memory_limit="60GB", **daskkws): #need 60 for all
        sm = 'poly'
        level_un = f"uncertainties{ens_name}-{sm}-2015"

        for var in ens_part.data_vars:
            if not pcat.exists_in_cat(processing_level=level_un, variable=var,
                                      domain=domain):
                print(f"Computing {level_un} {var}")
                ens_part = pcat.search(processing_level=level_part,
                                       variable=var,
                                       domain=domain
                                       ).to_dataset(**CONFIG['tdd'])

                # slice time
                ens_part = ens_part.sel(time=slice('2015', '2100'))

                _, uncertainties = xc.ensembles.general_partition(ens_part[var], sm=sm)
                uncertainties.attrs['partition_fit'] = sm
                uncertainties = uncertainties.to_dataset(name=var)
                uncertainties.attrs = ens_part.attrs
                uncertainties.attrs['cat:processing_level'] = level_un
                uncertainties.attrs['cat:variable'] = var
                xs.save_and_update(ds=uncertainties,
                                   pcat=pcat,
                                   path=CONFIG['paths']['output'],
                                   )
