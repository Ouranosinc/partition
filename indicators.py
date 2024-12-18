""" compute indicators for partition """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from dask.distributed import Client
import atexit
import xarray as xr
from xscen import CONFIG
import numpy as np

path = 'config/path_part.yml'
config = 'config/config_part.yml'
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)


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
    daskkws = CONFIG['dask'].get('client', {})
    atexit.register(xs.send_mail_on_exit, subject="Partition indicators")

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    with Client(n_workers=3, threads_per_worker=5, memory_limit="8GB", **daskkws):

        dict_sim = xs.search_data_catalogs()

        mod = xs.indicators.load_xclim_module(CONFIG["indicators"]["module"])

        for did, dc in dict_sim.items():
            *_, last = mod.iter_indicators()
            if not pcat.exists_in_cat(id=did, variable=last[0], xrfreq='YS-JAN',
                                      processing_level="indicators", domain='QC'):
                print('Computing indicators for ', did)
                chunks = {'rlat': 50, 'rlon': 50, 'time': 1460} if 'R2' in did else {
                    'lat': 50, 'lon': 50, 'time': 1460}

                # PCIC R2 data don't have lat and lon coords. Need to fix it.
                if 'MBCn-R2' in did or 'BCCAQv2-R2' in did:
                    ds_ext = xs.extract_dataset(catalog=dc,
                                                xr_open_kwargs={'chunks': chunks})['D']

                    template = pcat.search(bias_adjust_project='ESPO-G6-R2',
                                           variable='tg_mean', experiment='ssp370',
                                           source='MIROC6', domain='QC').to_dataset()

                    if (template.rlat.drop_vars(
                            'rotated_pole').equals(ds_ext.rlat) and
                            template.rlon.drop_vars(
                                'rotated_pole').equals(ds_ext.rlon)):
                        ds_ext['rlat'] = template['rlat']
                        ds_ext['rlon'] = template['rlon']
                        ds_ext = ds_ext.assign_coords(
                            {"lon": (('rlat', 'rlon'), template['lon'].data)})
                        ds_ext = ds_ext.assign_coords(
                            {"lat": (('rlat', 'rlon'), template['lat'].data)})
                        ds_ext['lat'].attrs = template['lat'].attrs
                        ds_ext['lon'].attrs = template['lon'].attrs
                    else:
                        print('rlat/rlon of template are not the same as the dataset.')
                elif 'reference' in did:
                    ds_ext = xs.extract_dataset(catalog=dc,
                                                xr_open_kwargs={'chunks': chunks})['D']
                    coords = CONFIG['coords'][ds_ext.attrs['cat:source']]
                    ds_ext = xs.utils.unstack_fill_nan(ds_ext,
                                                       coords=coords)
                else:
                    ds_ext = xs.extract_dataset(catalog=dc,
                                                region=CONFIG['region'],
                                                xr_open_kwargs={'chunks': chunks})['D']

                for name, ind in mod.iter_indicators():

                    # Get the freq and var names to check if they are already computed
                    outfreq = ind.injected_parameters["freq"].replace('YS', 'YS-JAN')
                    outnames = [cfatt["var_name"] for cfatt in ind.cf_attrs]
                    if not pcat.exists_in_cat(id=did, domain='QC',
                                              variable=outnames, xrfreq=outfreq,
                                              processing_level="indicators", ):
                        print(name)

                        _, ds_ind = xs.compute_indicators(
                            ds=ds_ext,
                            indicators=[(name, ind)],
                        ).popitem()

                        # add attrs for cf-xarray
                        for c in ['rlon', 'lon']:
                            if c in ds_ind.dims:
                                ds_ind[c].attrs['axis'] = 'X'

                        for c in ['rlat', 'lat']:
                            if c in ds_ind.dims:
                                ds_ind[c].attrs['axis'] = 'Y'

                        # to avoid issue when saving
                        for c in ds_ind.coords:
                            ds_ind[c].encoding.pop('chunks', None)

                        # put all on same type of time index
                        if isinstance(ds_ind.indexes['time'], xr.CFTimeIndex):
                            ds_ind['time'] = ds_ind.indexes['time'].to_datetimeindex()

                        # fix xrfreq bc weird behavior in between changes in pandas
                        ds_ind.attrs['cat:xrfreq'] = 'YS-JAN'

                        xs.save_and_update(ds=ds_ind,
                                           pcat=pcat,
                                           path=CONFIG['paths']['indicators'],
                                           save_kwargs=dict(
                                               rechunk={'time': -1, 'X': 50, 'Y': 50})
                                           )
    # add reference and adjustment to the catalog
    df = pcat.df.copy()
    pcat.df['adjustment'] = df.apply(add_col, axis=1, d=CONFIG['translate']['adjustment'])
    pcat.df['reference'] = df.apply(add_col, axis=1, d=CONFIG['translate']['reference'])
    pcat.update()
