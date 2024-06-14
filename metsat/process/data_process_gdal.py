from osgeo_utils import *
from osgeo import gdal
import struct

try: 
    dataset = gdal.Open("./../metsat/MSG2-SEVI-MSG15-0100-NA-20240609025740.532000000Z-NA/MSG2-SEVI-MSG15-0100-NA-20240609025740.532000000Z-NA.nat", gdal.GA_ReadOnly)
except gdal.Error as error: print(error)

print("Driver: {}/{}".format(dataset.GetDriver().ShortName,
                            dataset.GetDriver().LongName))
print("Size is {} x {} x {}".format(dataset.RasterXSize,
                                    dataset.RasterYSize,
                                    dataset.RasterCount))
print("Projection is {}".format(dataset.GetProjection()))
geotransform = dataset.GetGeoTransform()
if geotransform:
    print("Origin = ({}, {})".format(geotransform[0], geotransform[3]))
    print("Pixel Size = ({}, {})".format(geotransform[1], geotransform[5]))

bands = []
for i in range(1, dataset.RasterCount+1):
    bands = dataset.GetRasterBand(i)
    print("Band Type={}".format(gdal.GetDataTypeName(bands.DataType)))
    min = bands.GetMinimum()
    max = bands.GetMaximum()
    if not min or not max:
        (min,max) = bands.ComputeRasterMinMax(True)
    print("Min={:.3f}, Max={:.3f}".format(min,max))

    if bands.GetOverviewCount() > 0:
        print("Band has {} overviews".format(bands.GetOverviewCount()))

    if bands.GetRasterColorTable():
        print("Band has a color table with {} entries".format(bands.GetRasterColorTable().GetCount()))
    scanline = bands.ReadRaster(xoff=0, yoff=0,
                        xsize=bands.XSize, ysize=1,
                        buf_xsize=bands.XSize, buf_ysize=1,
                        buf_type=gdal.GDT_Float32)

    tuple_of_floats = struct.unpack('f' * bands.XSize, scanline)
    
