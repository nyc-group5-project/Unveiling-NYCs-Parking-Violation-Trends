import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


df=spark.read.parquet("s3://nycgroup05datalake/fiveyearsnycdata/",header=True)

df.printSchema()
df1=df

drop_column=['Street Code1','Street Code2','Street Code3','Violation Location','issuer precinct','issuer code','issuer command','issuer squad',
'time first observed','intersecting street','date first observed','violation legal code','unregistered vehicle?','vehicle year','meter number',
'violation post code','violation description','hydrant violation','double parking violation','Vehicle Expiration Date','No Standing or Stopping Violation']

df1=df1.drop(*drop_column)

from pyspark.sql.functions import to_date, expr

df1 = df1.withColumn('Issue Date', to_date(expr("substr(`Issue Date`, 1, 10)"), 'MM/dd/yyyy'))

from pyspark.sql.functions import col

df1 = df1.filter((col('Issue Date') >= '2018-07-01') & (col('Issue Date') <= '2023-06-30'))

from pyspark.sql.types import LongType

df1 = df1 \
    .withColumn("Summons Number", col("Summons Number").cast(LongType())) \
    .withColumn("Violation Code", col("Violation Code").cast(LongType())) \
	.withColumn("Law Section", col("Law Section").cast(LongType()))
df3=df1

df3.filter(col('Violation County').isNull()).count()

values_to_drop = ['F', 'A', 'MS','P','K   F',108,'ABX']

df3 = df3.filter(~col('Violation County').isin(values_to_drop))

from pyspark.sql.functions import when

df3 = df3.withColumn('Violation County',
                     when(df3['Violation County'] == 'NY', 'Manhattan')
                     .when(df3['Violation County'] == 'MN', 'Manhattan')
                     .when(df3['Violation County'] == 'Q', 'Queens')
                     .when(df3['Violation County'] == 'QN', 'Queens')
                     .when(df3['Violation County'] == 'K', 'Brooklyn')
                     .when(df3['Violation County'] == 'BK', 'Brooklyn')
                     .when(df3['Violation County'] == 'ST', 'Richmond')
                     .when(df3['Violation County'] == 'R', 'Richmond')
                     .when(df3['Violation County'] == 'BX', 'Bronx')
                     .when(df3['Violation County'] == 'QNS', 'Queens')
                     .when(df3['Violation County'] == 'Qns', 'Queens')
                     .when(df3['Violation County'] == 'QUEEN', 'Queens')
                     .when(df3['Violation County'] == 'Rich', 'Richmond')
                     .when(df3['Violation County'] == 'RICH', 'Richmond')
                     .when(df3['Violation County'] == 'KINGS', 'Brooklyn')
                     .when(df3['Violation County'] == 'Kings', 'Brooklyn')
                     .when(df3['Violation County'] == 'BRONX', 'Bronx')
                     .when(df3['Violation County'] == 'RICHM', 'Richmond')    
                     .otherwise(df3['Violation County'])
                     )
					 

df3 = df3.withColumn('Registration State',
                     when(df3['Registration State'] == 'MN', 'Minnesota')
                     .when(df3['Registration State'] == 'MX', 'Mexico')
                     .when(df3['Registration State'] == 'DC', 'District of Columbia')
                     .when(df3['Registration State'] == 'GV', 'Guanajuato (Mexico)')
                     .when(df3['Registration State'] == 'QB', 'Queretaro (Mexico)')
                     .when(df3['Registration State'] == 'MD', 'Maryland')
                     .when(df3['Registration State'] == 'DE', 'Delaware')
                     .when(df3['Registration State'] == 'MO', 'Missouri')
                     .when(df3['Registration State'] == 'IL', 'Illinois')
                     .when(df3['Registration State'] == 'MS', 'Mississippi')
                     .when(df3['Registration State'] == 'SD', 'South Dakota')
                     .when(df3['Registration State'] == 'ON', 'Ontario (Canada)')
                     .when(df3['Registration State'] == 'AK', 'Alaska')
                     .when(df3['Registration State'] == 'UT', 'Utah')
                     .when(df3['Registration State'] == 'HI', 'Hawaii')
                     .when(df3['Registration State'] == 'NS', 'Nova Scotia (Canada)')
                     .when(df3['Registration State'] == 'CA', 'California')
                     .when(df3['Registration State'] == 'CT', 'Connecticut')
                     .when(df3['Registration State'] == 'NC', 'North Carolina')
                     .when(df3['Registration State'] == 'ME', 'Maine')
                     .when(df3['Registration State'] == 'NM', 'New Mexico')
                     .when(df3['Registration State'] == 'IA', 'Iowa')
                     .when(df3['Registration State'] == 'AR', 'Arkansas')
                     .when(df3['Registration State'] == 'AZ', 'Arizona')
                     .when(df3['Registration State'] == 'LA', 'Louisiana')
                     .when(df3['Registration State'] == 'NJ', 'New Jersey')
                     .when(df3['Registration State'] == 'NT', 'Northwest Territories (Canada)')
                     .when(df3['Registration State'] == 'WI', 'Wisconsin')
                     .when(df3['Registration State'] == 'MT', 'Montana')
                     .when(df3['Registration State'] == 'MB', 'Manitoba (Canada)')
                     .when(df3['Registration State'] == 'NY', 'New York')
                     .when(df3['Registration State'] == 'FL', 'Florida')
                     .when(df3['Registration State'] == 'PR', 'Puerto Rico')
                     .when(df3['Registration State'] == 'KY', 'Kentucky')
                     .when(df3['Registration State'] == 'WY', 'Wyoming')
                     .when(df3['Registration State'] == 'BC', 'British Columbia (Canada)')
                     .when(df3['Registration State'] == 'MI', 'Michigan')
                     .when(df3['Registration State'] == 'NV', 'Nevada')
                     .when(df3['Registration State'] == 'ID', 'Idaho')
                     .when(df3['Registration State'] == 'VT', 'Vermont')
                     .when(df3['Registration State'] == 'WA', 'Washington')
                     .when(df3['Registration State'] == 'ND', 'North Dakota')
                     .when(df3['Registration State'] == 'AL', 'Alabama')
                     .when(df3['Registration State'] == 'IN', 'Indiana')
                     .when(df3['Registration State'] == 'TN', 'Tennessee')
                     .when(df3['Registration State'] == 'TX', 'Texas')
                     .when(df3['Registration State'] == 'GA', 'Georgia')
                     .when(df3['Registration State'] == 'CO', 'Colorado')
                     .when(df3['Registration State'] == 'OK', 'Oklahoma')
                     .when(df3['Registration State'] == 'SC', 'South Carolina')
                     .when(df3['Registration State'] == 'OR', 'Oregon')
                     .when(df3['Registration State'] == 'VA', 'Virginia')
                     .when(df3['Registration State'] == 'RI', 'Rhode Island')
                     .when(df3['Registration State'] == 'NH', 'New Hampshire')
                     .when(df3['Registration State'] == 'NE', 'Nebraska')
                     .when(df3['Registration State'] == 'OH', 'Ohio')
                     .when(df3['Registration State'] == 'PA', 'Pennsylvania')
                     .when(df3['Registration State'] == 'SK', 'Saskatchewan (Canada)')
                     .when(df3['Registration State'] == 'AB', 'Alberta (Canada)')
                     .when(df3['Registration State'] == 'PE', 'Prince Edward Island (Canada)')
                     .when(df3['Registration State'] == 'WV', 'West Virginia')
                     .when(df3['Registration State'] == 'MA', 'Massachusetts')
                     .when(df3['Registration State'] == 'KS', 'Kansas')
                     .when(df3['Registration State'] == 'NB', 'New Brunswick (Canada)')
                     .when(df3['Registration State'] == 'YT', 'Yukon Territory (Canada)')
                     .when(df3['Registration State'] == 'NF', 'Newfoundland (Canada)')
					 .when(df3['Registration State'] == 'DP', 'U.S. State Dept')
					 .when(df3['Registration State'] == 'FO', 'Foreign')
                    )



df3 = df3.withColumn('Issuing Agency',
                     when(df3['Issuing Agency'] == 'M', 'TRANSIT AUTHORITY')
                     .when(df3['Issuing Agency'] == 'U', 'PARKING CONTROL UNIT')
                     .when(df3['Issuing Agency'] == 'D', 'DEPARTMENT OF BUSINESS SERVICES')
                     .when(df3['Issuing Agency'] == 'B', 'TRIBOROUGH BRIDGE AND TUNNEL POLICE')
                     .when(df3['Issuing Agency'] == 'S', 'DEPARTMENT OF SANITATION')
                     .when(df3['Issuing Agency'] == 'T', 'TRAFFIC')
                     .when(df3['Issuing Agency'] == 'Z', 'METRO NORTH RAILROAD POLICE')
                     .when(df3['Issuing Agency'] == 'P', 'POLICE DEPARTMENT')
                     .when(df3['Issuing Agency'] == 'H', 'HOUSING AUTHORITY')
                     .when(df3['Issuing Agency'] == 'F', 'FIRE DEPARTMENT')
                     .when(df3['Issuing Agency'] == 'V', 'DEPARTMENT OF TRANSPORTATION')
                     .when(df3['Issuing Agency'] == 'O', 'NYS COURT OFFICERS')
                     .when(df3['Issuing Agency'] == 'C', 'CON RAIL')
                     .when(df3['Issuing Agency'] == 'X', 'OTHER/UNKNOWN AGENCIE')
                     .when(df3['Issuing Agency'] == '1', 'NYS OFFICE OF MENTAL HEALTH POLICE')
                     .when(df3['Issuing Agency'] == 'K', 'PARKS DEPARTMENT')
                     .when(df3['Issuing Agency'] == 'E', 'BOARD OF ESTIMATE')
                     .when(df3['Issuing Agency'] == 'A', 'PORT AUTHORITY')
                     .when(df3['Issuing Agency'] == 'R', 'NYC TRANSIT AUTHORITY MANAGERS')
                     .when(df3['Issuing Agency'] == 'N', 'NYS PARKS POLICE')
                     .when(df3['Issuing Agency'] == 'Y', 'HEALTH AND HOSPITAL CORP. POLICE')
                     .when(df3['Issuing Agency'] == 'L', 'LONG ISLAND RAILROAD')
                     .when(df3['Issuing Agency'] == 'G', 'TAXI AND LIMOUSINE COMMISSION')
                     .when(df3['Issuing Agency'] == '3', 'ROOSEVELT ISLAND SECURITY')
                     .when(df3['Issuing Agency'] == 'J', 'AMTRAK RAILROAD POLICE')
                     .when(df3['Issuing Agency'] == 'W', 'HEALTH DEPARTMENT POLICE')
                     .when(df3['Issuing Agency'] == 'I', 'STATEN ISLAND RAPID TRANSIT POLICE')
                     .when(df3['Issuing Agency'] == '8', 'SEA GATE ASSOCIATION POLICE')
                     .when(df3['Issuing Agency'] == 'Q', 'DEPARTMENT OF CORRECTION')
                     .when(df3['Issuing Agency'] == '4', 'WATERFRONT COMMISSION OF NY HARBOR')
                     .when(df3['Issuing Agency'] == '5', 'SUNY MARITIME COLLEGE')
                     .when(df3['Issuing Agency'] == '9', 'NYC OFFICE OF THE SHERIFF')
                    )

heavy_vehicle_keywords = [
    "TRAC", "TRK", "TANK", "FLAT", "DUMP", "TR/C", "WG", "SEMI", "TR/T", "TRL", "TK", "TRAIL", 
    "TRAI", "TRUC", "CARG", "CMIX", "T/CR", "TR/E", "CARR", "TR", "VAN", "TRUCK", "TOW", "TRACT", 
    "DLR", "TRLR", "BUS", "FIRE", "GARB", "REF", "LNCH", "REFG", "MOT", "U", "SNOW", "H/WH", 
    "BU", "M/H", "UHAU", "CHAS", "BULK", "CB", "LTRL", "SCOO", "PSVA", "B/V", "DOLL", "HEAVY", 
    "EQ", "RAC", "CB", "RL", "RACK", "REFR", "CAN", "COOL", "TC", "STOR", "W/CR", "FRUE", "STAK", 
    "FURN", "MOV", "STK", "COM", "W/VA", "STAB", "ELEV", "SCH", "RID", "LOADE", "BLD", "W/CA", 
    "B/GR", "BOOM", "O/SE", "SCAF", "CONCR", "GROU", "ROOF", "ICE", "B/F", "SAND", "RECR", "DROPS", 
    "PORTA", "EXCAV", "WOOD", "CONST", "FIR", "LIM", "LIFT", "CATER", "SCHB", "RIG", "DR", "W/DR", 
    "G/GR", "G/GS", "SPRI", "FLEET", "SPCI", "ME", "AIR", "TIRE", "DETEC", "SALT", "LOAD", "D/CA", 
    "D/CW", "INDU", "SCRA", "W/GA", "W/FO", "W/OI", "D/CE", "M/SK"
]

two_wheeler = ["MCY", "MC", "MOPD", "MOTO", "MOT", "ATV", "SC", "SKEB", "TRIC", "UT", "MOP", "SCOO", "SCOT", "MOTOR","MOPD", "MOPG", "MOPP", "MOPS", "MOPH", "MCY", "MCR", "SPO", "SPOF", "SPS", "SPSG", "SPSL", "SPV", "MCP", "TOM", "MRV", "MRVC", "MFW", "MB", "MRW", "OMS", "OTR", "OTRC", "OTH", "TOW", "TO", "TOR", "TRS", "TSR", "TT", "TTM", "TTN", "TTP", "TTS", "TUBE", "TUK", "TURT", "TURU", "TUV", "TWIN", "TWOS", "TWTR"]
four_wheeler=['CXMI', 'TRUK', '2C', 'MOT', 'ZER', '4SLE', 'TRAL', 'ANXS', 'CT', 'ANB', 'DEL', 'OMR', 'AMG', 'HNH', 'WSR', 'VEH-', 'DELE', 'PICH', 'AMR', 'IX', 'TRY', 'GV', 'NC', 'MINI', 'UTIL', 'BL', 'BUS.', 'OTHE', 'CV', 'SN/P', 'LIMO', 'TOW', 'TV', 'POWE', 'FOUR', 'CMHI', 'TRRL', 'HIWH', '5D', 'INCE', 'ET', 'KIA', 'ANL', 'JEEP', 'AO', 'ONAT', 'LMB', 'TRAILER', 'MX', 'BILR', 'TRAU', 'NXC', 'CKNE', 'CAM', 'STWA', 'SNL', 'AIAI', '2DSD', 'PAS', 'OU', 'CL', 'WORK', 'CONY', 'FGHT', 'CH', 'BMS', 'TWOD', 'ALL', 'HUMM', 'OLHE', 'BLUE', 'FRT', 'EXPL', 'HOUS', 'ORM', 'OLX', 'OTNE', 'AS', 'XRE', 'RF', 'BX', 'RJZL', '2HB', 'FIRE', 'N/S', 'CXI', 'ANEL', 'ONRL', '4DSE', 'TU', 'HYUN', 'RANG', 'ANOE', 'MH', 'HY', 'TLE', 'OVX', 'PASS', 'MD', 'MN', 'CMRU', 'SCOO', 'TRIM', 'ROS', 'CUSH', '4S', 'TERL', 'REG', 'SUBN', 'NS', 'PORS', 'NG', 'RPLC', 'GARB', 'TRAD', 'TRK.', 'H/TR', 'EC', 'CAMI', 'TARI', 'C4', 'OVL', 'SP', 'EXPE', 'TRLR', 'SPOR', 'AGI', 'NER', '2 DR', 'TBL', 'HC', 'P-U', 'RMA', 'UTLI', 'AM', 'MOC', 'TRLA', 'MO', 'AK', 'SHUT', 'STRA', 'UT', '4DSW', 'BOL', 'BUD', 'DEFI', 'MDEL', 'B', 'OSOO', 'APPP', 'CMR', 'GONV', 'REFG', 'GY', 'BOAT', '2HT', 'RRB', 'REK', 'IML', 'SRIB', 'ANOL', 'T', 'CW', 'TRUL', 'IMS', 'SWT', 'COPE', 'CA', '8V', 'TRL.', 'TPAC', 'ISUZ', 'PICK', '4C', 'OVC', 'MOCL', 'REFU', 'TANK', 'CLLM', 'TAHO', 'SD', '2F', 'PICKU', 'SUW', 'MCL', 'MCTO', 'PU', 'LTR', 'W/SR', 'OMU', 'THR', 'BS', 'HRSE', 'M', 'OTM', 'CRLA', 'AEL', 'MACK', '2DSE', 'TLK', 'PATH', 'TRL', 'RD', 'MOPE', 'ULT', 'CONV', 'CRAF', 'OOCR', 'PRK', 'TRAIL', 'G', 'OVE', 'JOII', 'SAT', 'RAIL', 'TRF', 'MTCY', 'SV', 'OB', 'STAW', 'RTUC', 'ON', 'TAA', 'THW', 'MTCL', 'OUTB', 'FLTB', 'COOP', 'WAGON', 'OOL', 'GLAT', 'COUP', 'BIS', 'RCE', 'II', 'PCL', 'MR', 'C', 'HYNH', 'YW', 'ASRC', 'LUXV', 'TRHR', 'DELIVERY', 'TRAV', 'OORL', 'BK', 'BURG', 'REF', 'FORD', 'VAN', 'SDN', 'CNIR', 'RXM', 'TRUCK', 'FODO', 'EGOL', 'SL', '2Z', 'RGR', 'ILI', 'RAD', 'LSUO', 'R', 'UV', 'TLC', 'MS', 'DCOM', 'TRCL', 'ORS', 'MERC', 'BN', 'BUS', 'MACY', 'C/CB', 'FREG', 'UITL', 'TS', 'TREA', 'PT', 'OUXL', '4DSD', 'TAXI', 'CXRG', '4 DR', 'CHRY', 'TRACT', '4DRS', 'CI', 'H/IN', 'SRF', 'DOZ', 'MCYL', 'XXM', 'IDLL', 'S', 'VW', 'WAG', 'CZ', 'U', 'AMF', 'COHV', 'PICK-UP', 'ILSJ', 'UM', 'DELV', 'PCV', 'OSOL', 'RBM', 'UHAU', 'TRKC', 'YK', 'HWO', 'SM', 'OML', 'WB', 'REFN', 'FLAT', 'P/SH', 'OII', 'CMIX', 'BOXT', 'YY', 'MOBL', '4DHT', 'PC', 'L', 'SUL', 'OMRL', 'PK', 'NOL', 'SCL', 'BOX', 'BLBI', 'ULIT', 'MEC', '4T', '4DHB', 'STAG', 'H/WH', 'CUST', 'LNCH', 'PINN', 'LO', 'REFF', 'D', 'RFEF', 'EO', 'STAK', '34PU', 'POLE', 'CNI', 'STAR', 'PICKUP', 'JUTI', 'Truc', '2CV', 'MOTC', 'PLSH', 'Four Wheeler', '4 WH', 'COU', 'DECA', 'ADP', 'CX', 'CMC', 'MODC', '4 DO', 'MP', 'IC', 'TKR', 'SS', 'TAX', 'WAGO', '12PU', 'MK', 'VHL', 'FLFT', 'TRD', 'HWH', 'AMB', 'ONE', 'LL', 'GRAN', 'OIM', 'HOW', 'R/RD', 'IJ', 'TRAC', 'OLO', 'CM', 'SCHO', '3D', 'ARC', 'GOUR', 'APP', 'BLAC']
df4=df3
df4 = df4.withColumn('Vehicle Size',
                      when(df4['Vehicle Body Type'].isin(heavy_vehicle_keywords), 'Heavy Vehicle')
                      .when(df4['Vehicle Body Type'].isin(two_wheeler), 'Two Wheeler')
                      .when(df4['Vehicle Body Type'].isin(four_wheeler), 'Four Wheeler')
                      .otherwise('Others'))
					  

from pyspark.sql.functions import substring, concat, lit

df11=df4.filter(~substring(col('Violation Time'),-1,1).isin('A','P'))

df11 = df11.withColumn("Violation Time",
                    concat(substring(col("Violation Time"), 1, 2), lit(":"), substring(col("Violation Time"), 3, 2)))


df12=df4.filter(substring(col('Violation Time'),-1,1)=='A')		
df12 = df12.withColumn("Violation Time",
                    concat(substring(col("Violation Time"), 1, 2), lit(":"), substring(col("Violation Time"), 3, 2)))

df13=df4.filter(substring(col('Violation Time'),-1,1)=='P')
df13 = df13.withColumn('Hour', when(substring('Violation Time', -1, 1) == 'P', 
                                     concat(substring('Violation Time', 1, 2), lit(' ')).cast("int") + 12)
                      .otherwise(substring('Violation Time', 1, 2)))
				
df13 = df13.withColumn('Violation Time', concat(col('Hour'), lit(":"), substring(col('Violation Time'), 3, 2)))
					  
df13=df13.drop('Hour')
df5=df11.union(df12)
df5=df5.union(df13)

#from pyspark.sql.functions import date_format
#df5 = df5.withColumn("Violation Time", substring(col("Violation Time"), 1, 5).cast("timestamp"))

from pyspark.sql.functions import when, col

df5 = df5.withColumn('violation in front of or opposite',
                     when(col('violation in front of or opposite') == 'R', 'Right')
                     .when(col('violation in front of or opposite') == 'F', 'Front')
                     .when(col('violation in front of or opposite') == 'O', 'Opposite')
                     .when(col('violation in front of or opposite').isNull(), 'Others')
                     .when(col('violation in front of or opposite') == 'X', 'Others')
                     .when(col('violation in front of or opposite') == 'I', 'Intersection'))
df6=df5
df6 = df6.withColumn('Plate Type',
                     when(col('Plate Type') == 'CCK', 'Commercial Cargo Truck')
                     .when(col('Plate Type') == 'LMB', 'Limousine')
                     .when(col('Plate Type') == 'CLG', 'College/University Vehicle')
                     .when(col('Plate Type') == 'SPC', 'Special Purpose Vehicle')
                     .when(col('Plate Type') == 'APP', 'Apparel/Uniform Vendor')
                     .when(col('Plate Type') == 'AMB', 'Ambulance')
                     .when(col('Plate Type') == 'CMH', 'Community Health Vehicle')
                     .when(col('Plate Type') == 'IRP', 'International Registration Plan Vehicle')
                     .when(col('Plate Type') == 'OMF', 'Official Mail Facility Vehicle')
                     .when(col('Plate Type') == 'OML', 'Official Military Vehicle')
                     .when(col('Plate Type') == 'ORM', 'Official Religious Vehicle')
                     .when(col('Plate Type') == 'OMR', 'Official Medical Vehicle')
                     .when(col('Plate Type') == 'SCL', 'School Bus (City)')
                     .when(col('Plate Type') == 'OMV', 'Official Medical Vehicle (Private)')
                     .when(col('Plate Type') == 'STG', 'Stagecoach/Entertainment Vehicle')
                     .when(col('Plate Type') == 'JCL', 'Judicial/County Vehicle')
                     .when(col('Plate Type') == 'PPH', 'Public Health Vehicle')
                     .when(col('Plate Type') == 'LMA', 'Livery Motorcycle')
                     .when(col('Plate Type') == 'DLR', 'Dealer Vehicle')
                     .when(col('Plate Type') == 'SRN', 'Senior Citizen Vehicle')
                     .when(col('Plate Type') == 'CMB', 'Commercial Bus')
                     .when(col('Plate Type') == 'VAS', "Visitor's Authority Vehicle")
                     .when(col('Plate Type') == 'SUP', "Supervisor's Vehicle")
                     .when(col('Plate Type') == 'ORC', 'Orchestra/Entertainment Vehicle')
                     .when(col('Plate Type') == 'LTR', 'Light Rail Vehicle')
                     .when(col('Plate Type') == 'SRF', 'Street/Road Maintenance Vehicle')
                     .when(col('Plate Type') == 'LUA', 'Land Use Agency Vehicle')
                     .when(col('Plate Type') == 'MCL', 'Miscellaneous Commercial Vehicle')
                     .when(col('Plate Type') == 'PAS', 'Passenger Car')
                     .when(col('Plate Type') == 'PHS', 'Public Health Service Vehicle')
                     .when(col('Plate Type') == 'GSM', 'Gas Meter Vehicle')
                     .when(col('Plate Type') == 'HIR', 'Hiring Vehicle (Taxis, etc.)')
                     .when(col('Plate Type') == 'PSD', 'Public Safety Department Vehicle')
                     .when(col('Plate Type') == 'HIS', 'Historic Vehicle')
                     .when(col('Plate Type') == 'GSC', 'General Services Vehicle')
                     .when(col('Plate Type') == 'TRA', 'Tractor Trailer')
                     .when(col('Plate Type') == 'JSC', 'Job Site Construction Vehicle')
                     .when(col('Plate Type') == 'OMO', 'Official Military Vehicle (Other)')
                     .when(col('Plate Type') == 'NYA', 'New York Authority Vehicle')
                     .when(col('Plate Type') == 'CHC', 'Church/Religious Organization Vehicle')
                     .when(col('Plate Type') == 'RGC', 'Regional Government Vehicle')
                     .when(col('Plate Type') == 'HAM', 'Handicap Accessible Vehicle')
                     .when(col('Plate Type') == 'ORG', 'Organization Vehicle')
                     .when(col('Plate Type') == 'USC', 'Utility Service Company Vehicle')
                     .when(col('Plate Type') == 'ITP', 'Intercity Passenger Vehicle')
                     .when(col('Plate Type') == 'AGR', 'Agriculture Vehicle')
                     .when(col('Plate Type') == 'OMT', 'Official Military Trailer')
                     .when(col('Plate Type') == 'THC', 'Taxi and Limousine Commission Vehicle')
                     .when(col('Plate Type') == 'SPO', 'School Post Office Vehicle')
                     .when(col('Plate Type') == 'HAC', 'Handicapped Accessible Commercial Vehicle')
                     .when(col('Plate Type') == 'TOW', 'Tow Truck')
                     .when(col('Plate Type') == 'FPW', 'Fire Patrol Wagon')
                     .when(col('Plate Type') == 'NLM', 'Non-Livery Motorcycle')
                     .when(col('Plate Type') == 'MOT', 'Motorcycle')
                     .when(col('Plate Type') == 'CME', 'Commercial Meter Vehicle')
                     .when(col('Plate Type') == 'CBS', 'Commercial Bus Stand')
                     .when(col('Plate Type') == 'STA', 'State Agency Vehicle')
                     .when(col('Plate Type') == 'TRC', 'Tourist Coach')
                     .when(col('Plate Type') == '999', 'Unknown/Unidentified Plate Type')
                     .when(col('Plate Type') == 'JCA', 'Judicial/County Agency Vehicle')
                     .when(col('Plate Type') == 'CSP', 'City Special Vehicle')
                     .when(col('Plate Type') == 'AYG', 'Army National Guard Vehicle')
                     .when(col('Plate Type') == 'GAC', 'General Assembly Commission Vehicle')
                     .when(col('Plate Type') == 'VPL', 'Vehicle Protection League')
                     .when(col('Plate Type') == 'TRL', 'Trailer')
                     .when(col('Plate Type') == 'SOS', 'Secretary of State Vehicle')
                     .when(col('Plate Type') == 'FAR', 'Farm Vehicle')
                     .when(col('Plate Type') == 'RGL', 'Religious Organization Vehicle')
                     .when(col('Plate Type') == 'COM', 'Commercial Vehicle')
                     .when(col('Plate Type') == 'BOB', 'Bomb Squad Vehicle')
                     .when(col('Plate Type') == 'NYS', 'New York State Vehicle')
                     .when(col('Plate Type') == 'WUG', 'Water Utility Vehicle')
                     .when(col('Plate Type') == 'ATV', 'All Terrain Vehicle')
                     .when(col('Plate Type') == 'AGC', 'Agricultural Commercial Vehicle')
                     .when(col('Plate Type') == 'MED', 'Medical/Health Services Vehicle')
                     .when(col('Plate Type') == 'ARG', 'Argiculture/Agri Business Vehicle')
                     .when(col('Plate Type') == 'SEM', 'Semi-Trailer')
                     .when(col('Plate Type') == 'OMS', 'Official Military Sedan')
                     .when(col('Plate Type') == 'LMC', 'Limousine Commercial')
                     .when(col('Plate Type') == 'HOU', 'Household Goods Mover')
                     .when(col('Plate Type') == 'NYC', 'New York City Vehicle')
                     .when(col('Plate Type') == 'BOT', 'Bus Operator Taxicab')
                     .when(col('Plate Type') == 'MCD', 'Motor Coach Disposal')
                     .when(col('Plate Type') == 'JWV', 'Job Work Vehicle')
                     .when(col('Plate Type') == 'HSM', 'Historic Military Vehicle')
                     .when(col('Plate Type') == 'USS', 'US State Vehicle')
                     .when(col('Plate Type') == 'HIF', 'High Floor Bus')
                     .when(col('Plate Type') == 'ATD', 'All Terrain Delivery Vehicle')
                     .when(col('Plate Type') == 'TMP', 'Temporary Plate')
                     .when(col('Plate Type') == 'SNO', 'Snow Plow')
                     .when(col('Plate Type') == 'LOC', 'Local Government Vehicle')
                     .otherwise(col('Plate Type')))
					 

df6 = df6.withColumn('Vehicle Color',
                     when(col('Vehicle Color') == 'GY', 'Grey')
                     .when(col('Vehicle Color') == 'WH', 'White')
                     .when(col('Vehicle Color') == 'BK', 'Black')
                     .when(col('Vehicle Color') == 'BL', 'Blue')
                     .when(col('Vehicle Color') == 'RD', 'Red')
                     .when(col('Vehicle Color') == 'TN', 'Tan')
                     .when(col('Vehicle Color') == 'BR', 'Brown')
                     .when(col('Vehicle Color') == 'YW', 'Yellow')
                     .when(col('Vehicle Color') == 'GR', 'Green')
                     .when(col('Vehicle Color') == 'SIL', 'Silver')
                     .when(col('Vehicle Color') == 'OR', 'Orange')
                     .when(col('Vehicle Color') == 'GL', 'Gold')
                     .when(col('Vehicle Color') == 'MR', 'Maroon')
                     .when(col('Vehicle Color') == 'GN', 'Green')
                     .when(col('Vehicle Color') == 'LTGY', 'Light Grey')
                     .when(col('Vehicle Color') == 'WT', 'White')
                     .when(col('Vehicle Color') == 'LTG', 'Light Grey')
                     .when(col('Vehicle Color') == 'SL', 'Silver')
                     .when(col('Vehicle Color') == 'PR', 'Purple')
                     .when(col('Vehicle Color') == 'BGE', 'Beige')
                     .when(col('Vehicle Color') == 'DBL', 'Dark Blue')
                     .when(col('Vehicle Color') == 'PLE', 'Pale')
                     .when(col('Vehicle Color') == 'CRM', 'Cream')
                     .when(col('Vehicle Color') == 'PNK', 'Pink')
                     .when(col('Vehicle Color') == 'TEA', 'Teal')
                     .when(col('Vehicle Color') == 'COPPE', 'Copper')
                     .when(col('Vehicle Color') == 'BIEGE', 'Beige')
                     .when(col('Vehicle Color') == 'AME', 'American')
                     .when(col('Vehicle Color') == 'AQ', 'Aqua')
                     .when(col('Vehicle Color') == 'BUG', 'Burgundy')
                     .when(col('Vehicle Color') == 'TNG', 'Tangerine')
                     .when(col('Vehicle Color') == 'COM', 'Combination')
                     .when(col('Vehicle Color') == 'TRQ', 'Turquoise')
                     .when(col('Vehicle Color') == 'CL', 'Clear')
                     .when(col('Vehicle Color') == 'RUST', 'Rust')
                     .when(col('Vehicle Color') == 'BY', 'Baby')
                     .when(col('Vehicle Color') == 'MC', 'Metallic')
                     .when(col('Vehicle Color') == 'GAY', 'Gray')
                     .when(col('Vehicle Color') == 'LAVEN', 'Lavender')
                     .when(col('Vehicle Color') == 'PURPL', 'Purple')
                     .when(col('Vehicle Color') == 'YELL', 'Yellow')
                     .when(col('Vehicle Color') == 'MAROO', 'Maroon')
                     .when(col('Vehicle Color') == 'RDBK', 'Red and Black')
                     .when(col('Vehicle Color') == 'BLUE', 'Blue')
                     .when(col('Vehicle Color') == 'ALUMI', 'Aluminum')
                     .when(col('Vehicle Color') == 'GOLD', 'Gold')
                     .when(col('Vehicle Color') == 'TAN', 'Tan')
                     .when(col('Vehicle Color') == 'LTTN', 'Light Tan')
                     .when(col('Vehicle Color') == 'BIEGE', 'Beige')
                     .when(col('Vehicle Color') == 'BRZ', 'Bronze')
                     .when(col('Vehicle Color') == 'PUR', 'Purple')
                     .when(col('Vehicle Color') == 'TEAL', 'Teal')
                     .when(col('Vehicle Color') == 'BGE', 'Beige')
                     .when(col('Vehicle Color') == 'ONG', 'Orange')
                     .when(col('Vehicle Color') == 'LBL', 'Light Blue')
                     .when(col('Vehicle Color') == 'PEWTE', 'Pewter')
                     .when(col('Vehicle Color') == 'YWBL', 'Yellow and Blue')
                     .when(col('Vehicle Color') == 'TAN.', 'Tan')
                     .when(col('Vehicle Color') == 'RED.', 'Red')
                     .when(col('Vehicle Color') == 'LT/GY', 'Light Grey')
                     .when(col('Vehicle Color') == 'BURGU', 'Burgundy')
                     .when(col('Vehicle Color') == 'LT/BL', 'Light Blue')
                     .when(col('Vehicle Color') == 'RDTN', 'Red and Tan')
                     .when(col('Vehicle Color') == 'BKOR', 'Black and Orange')
                     .otherwise('Others'))

df6 = df6.withColumn('Violation Code', when(col('violation code') == 0, 99).otherwise(col('Violation Code')))
 
			 
df6.coalesce(1).write \
	.format("parquet") \
	.option("header", "true") \
	.mode("overwrite") \
	.save("s3://nycgroup05datawarehouse/warehousenyc")