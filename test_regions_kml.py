from leaf_wrapper import LeafWrapper

def regions_from_kml(kml_file, n=10, prefix="region"):
    """
    Load a KML and return a dict of polygon regions:
    {
        'region1': {...},
        'region2': {...},
        ...
    }
    """
    wrapper = LeafWrapper(kml_file).load()
    regions_dict = wrapper.to_region_dict()  # keys = TARGET_FID

    out = {}
    for i in range(1, min(n, len(regions_dict)) + 1):
        region_data = regions_dict[i]
        out[f"{prefix}{i}"] = {
            "type": "Polygon",
            "coordinates": region_data["coordinates"],
        }

    return out



print("Test regions_from_kml:")
regions = regions_from_kml("C:\\Users\\aradar\\LEAF Files\\LEAF-Stack\\AfforestationSItesFixed.kml", n=3)
print(regions)