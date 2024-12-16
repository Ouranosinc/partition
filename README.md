# partition
Code for the paper "On the importance of the reference data: Uncertainty partitioning of bias-adjusted climate simulations over Quebec" by Lavoie et al. (submitted in 2024)


Fill in your paths in a path_part.yml file based on the path_part.yml.example file.
Confirm choices (such as ens_name)  in config.yml.


If starting from daily timeseries on the Ouranos server:
0. create_ic6_input_cat.ipynb (private)
1. indicators.py

If starting from annual indicators downloaded from https://doi.org/10.5281/zenodo.14397866 (if you don't know, it's probably this one):
1. create_cat.py


2. build_mask.py
3. regrid.py
4. gridpoints.py
5. maps.py
6. paper_figures.ipynb
