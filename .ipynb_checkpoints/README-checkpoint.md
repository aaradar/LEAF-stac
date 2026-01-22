# STAC_python



There are two options for running the STACk Python application: 1) Debug Mode and 2) Distributed Mode.

Debug Mode:
This mode is designed to run on a single machine (either personal or HPC). It is particularly useful for testing and troubleshooting smaller datasets. When the -d flag is set, the program will operate in debug mode, allowing for a more streamlined and less resource-intensive run.

Distributed Mode:
The distributed mode is intended for running on a cluster of Dask workers, typically in an HPC environment. This mode leverages parallel processing to handle larger datasets. The number of workers, memory allocation, and number of nodes can be configured. However, these settings are predefined within the program, and it is recommended to keep the default values unless you have a thorough understanding of the data and the worker setup. Adjusting these parameters without a proper grasp of the system may lead to suboptimal performance.

*Note #1*
For running the program on an HPC system, two environment variables must be set before execution. These variables are essential because the program makes frequent HTTP requests. To ensure these requests are fulfilled, the HPC node must have access to the necessary proxies.

The required environment variables are:

http_proxy
https_proxy
These variables allow the program to route its HTTP requests through the specified proxies, enabling seamless communication even within network-restricted environments.


```bash
export http_proxy=http://webproxy.science.gc.ca:8888/
export https_proxy=http://webproxy.science.gc.ca:8888/
```
*Note #2*

Before running the project, the path should be changed to where the source folder of the project resides. Thus, it require the project to be on HPC if running on HPC:

```bash
cd /../../../nrcan_geobase/work/dev/.../../STAC_python/imagery-composite
```

*Note #3*
If you are interested in working with Harmonized Landsat Data (HLS), it's important to know that these datasets are provided by the Earthdata website, which requires each user to have an account. Therefore, we recommend that you first create an account on the Earthdata website at (https://urs.earthdata.nasa.gov)

Afterward, you will need to provide your credentials in a .netrc file on your Linux system. By default, this file is located at ~/.netrc on non-Windows systems or ~/_netrc on Windows systems. If you'd like to use a different location, you can override the default path by setting the NETRC environment variable to the desired file path.

To create the .netrc file with your credentials, please run the following command in your terminal:

```bash

echo "machine urs.earthdata.nasa.gov login **your username** password **your password**" > ~/.netrc

```


*Here's an example of how to use STAC_python (Command line and Script):*

**Command line**
```bash
python main -y <year> -m <months> -t <tile_names> -o <out_folder> -s <sensor> -u <unit> -nby <nbyears> -pn <prod_names> -r <resolution> -proj <projection> -cl <cloud_cover> -sd <start_dates> -ed <end_dates> -nw <number_workers> -nm <node_memory> -n <nodes> -d  -et -ang
```
- `-y`, `--year`: Image acquisition year
- `-m`, `--months`: List of months included in the product (e.g., 5 6 7 8 9 10)
- `-t`, `--tile_names`: List of (sub-)tile names. Note that if you decide to go with subtiles, the -et flag should not be set.
- `-o`, `--out_folder`: Folder name for exporting
- `-s`, `--sensor`: Sensor type (e.g., 'S2_SR','HLSS30_SR', 'HLSL30_SR', 'HLS_SR') (Optional: default is S2_SR)
- `-u`, `--unit`: Data unit code (1 for TOA, 2 for surface reflectance) (Optional: default is 1)
- `-nby`, `--nbyears`: Positive integer for annual product, or negative for monthly product (Optional: default is -1)
- `-pn`, `--prod_names`: List of product names (e.g., 'mosaic', 'LAI', 'fCOVER') (Optional: default is ['LAI', 'fCOVER', 'fAPAR', 'Albedo'])
- `-r`, `--resolution`: Spatial resolution (Optional: default is 20). For HLS data, you need to work with a 30m resolution.
- `-proj`, `--projection`: Projection (e.g., 'EPSG:3979') (Optional: default is EPSG:3979)
- `-cl`, `--cloud_cover`:cloud_cover (Optional: default is 85.5)
- `-sd`, `--start_dates`: List of start dates (e.g., '2023-05-01'). This needs to be set if you have a start date other than the first day of the month you provided.
- `-ed`, `--end_dates`: List of end dates (e.g., '2023-05-01') (Optional: default is NA). This needs to be set if you have an end date other than the last day of the month you provided.
- `-nw`, `--number_workers`: The number of total dask workers to run the program. The number should be set based on the number of cores and physical nodes available for dask. (Optional: default is set based on the avaialbel stack items and debug mode)
- `-nm`, `--node_memory`: The amount of memory for each dask worker. The memory should be set based on the available memory on each node and number of dask workers running on each physical node (Optional: default is set based on the avaialbel stack items and debug mode)
- `-n`, `--nodes`:The number of physical nodes in distributed dask mode (Optional: default is set based on the avaialbel stack items and debug mode)
- `-d`, `--debug`: Run the program in debug mode. In the debug mode, dask will be creating its cluster on a single physical node.
- `-et`, `--entire_tile`: Mosaic the entire tile. By setting this argument, the program will be run for all 9 subtitles of the requested tile.
- `-ang`, `--include_angles`: Whether to include angle bands in the Mosicking process or not.
- `-ralt`, `--region_lat`: The latitude of the region of interest in the following order: Top-left, Top-right, Bottom-right, and Bottom-left.
- `-rlon`, `--region_lon`: The longitude of the region of interest in the following order: Top-left, Top-right, Bottom-right, and Bottom-left. 
- `-rc`, `--region_catalog`: The STAC Catalog Json file for the desired Region of interest.
- `-b`, `--bands`: The name of extra requested bands.
NOTE: You are required to either provide months or a customized start and end date for the temporal aspect. If you provide one or a list of months, the start date will be the first day of the first month, and the end date will be the last day of the last month.

For an example:

```bash
python main.py -y 2023 -t tile55  0 -sd 2023-05-01 -ed 2023-10-30 -o .../tile55_seasonal/ -et

```

```bash
python main.py -y 2023 -m 5 6 7 8 9 10 -t tile55  -o .../tile55_seasonal/ -et -ang

```



There are two ways to run the Imagery Composite Project. 

The first option is to define *your Region of Interest (ROI)* as a bounding box, using [Longitude and Latitude] coordinates.

In this case, you must provide the appropriate arguments for the bounding box (the coordinates), and refrain from using some arguments that are not applicable when working with this method.

When using this method, the following arguments should not be set:

```bash
-t <tile_names>: Since you are defining your own region of interest, specifying tile names will override the region you’ve set.
-et: This flag is only applicable when working with small tiles that will be mosaicked together at the end to form a larger tile. Since you're working with a defined region, this argument is not necessary.

```

By omitting these arguments, the project will correctly focus on the region you've specified.

The way you define your region of interest is by setting the latitude and longitude in the order of Top-Left, Top-Right, Bottom-Left, Bottom-Right.

For an example:

```bash

python main.py -y 2023 -sd 2023-05-01 -ed 2023-10-30 -o .../tile55_seasonal/ -rlon -113.1981 -111.9116 -107.4367 -108.4062 -rlat 55.8769 53.2369 53.8631 56.5546

```

In addition to providing the bounding box (the coordinates), you can provide a path to a JSON file that contains the region of interest specification, formatted according to the STAC Catalog Specification.


```bash

python main.py -y 2023 -sd 2023-05-01 -ed 2023-10-30 -o .../tile55_seasonal/ -rc imagery-composite/test_data/NB_SNB_2022_n.json

```

The second option is to work with the predefined tiles and subtiles in the project. For this purpose, we recommend checking the eoTileGrids.py source to find the tile of your interest.

In this case, you must provide the appropriate arguments for the tile name, and refrain from using some arguments that are not applicable when working with this method.

When using this method, the following arguments should not be set:

```bash
-rlat 
-rlon
-rc

```
By omitting these arguments, the project will correctly focus on the region you've specified. 
If you want to create a mosaic for the whole tile, please set the -et flag.

For an example:

```bash

python main.py -y 2023 -sd 2023-05-01 -ed 2023-10-30 -o .../tile55_seasonal/ -t tile42 -et

```
