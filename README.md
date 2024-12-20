# partition
Code for the paper "On the importance of the reference data: Uncertainty partitioning of bias-adjusted climate simulations over Quebec" by Lavoie et al. (submitted in 2024)

## Set up
1. Download data from https://doi.org/10.5281/zenodo.14397866 and unzip it.
2. Create environnment
```
conda env create -f environment.yml
mamba activate partition-env
pybabel compile -f -D xscen -d DIR/partition-env/lib/python3.11/site-packages/xscen/data 
```
Last line needed for xscen branch.

3. Fill in your paths in a path_part.yml file based on the path_part.yml.example file.
4. Confirm choices (such as ens_name)  in config.yml.

## Run the code 
If starting from daily timeseries on the Ouranos server:
    0. create_ic6_input_cat.ipynb (private)
    1. indicators.py
    2. build_mask.py 

If starting from annual indicators downloaded data (if you don't know, it's probably this one):
    1. create_cat.py



3. regrid.py
4. gridpoints.py
5. maps.py
6. paper_figures.ipynb


#TODO: maybe put maks in zenodo instead.