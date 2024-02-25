from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, expr,col,when,substring, concat, lit,regexp_replace,to_timestamp
from pyspark.sql.types import LongType
from pyspark.sql.functions import hour, minute, date_format
# Create a SparkSession
spark = SparkSession.builder \
    .appName("test1") \
    .getOrCreate()

# Reading merged parquet file	
df=spark.read.parquet("s3://nycgroup05datalake/fiveyearsnycdata/",header=True)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Dropping unnecessary columns
drop_column=['Street Code1','Street Code2','Street Code3','Vehicle Expiration Date','Violation Location','issuer precinct',
             'issuer code','issuer command','issuer squad','time first observed','House Number','intersecting street','date first observed','violation legal code','unregistered vehicle?','vehicle year','meter number','violation post code','violation description','No Standing or Stopping Violation','hydrant violation','double parking violation','From Hours In Effect','To Hours In Effect','Days Parking In Effect']
df=df.drop(*drop_column)

# Convert Issue Date datatype from string to date type
df = df.withColumn('Issue Date', to_date(expr("substr(`Issue Date`, 1, 10)"), 'MM/dd/yyyy'))

# Filter data for the required date range
df = df.filter((col('Issue Date') >= '2018-07-01') & (col('Issue Date') <= '2023-06-30'))

# Clean 'law section' column
df = df.withColumn('Law Section', regexp_replace(col('Law Section'), ',', ''))

# Cast necessary columns to appropriate types
df = df \
    .withColumn("Summons Number", col("Summons Number").cast(LongType())) \
    .withColumn("Violation Code", col("Violation Code").cast('int')) \
	.withColumn('violation precinct',col('violation precinct').cast('int')) \
	.withColumn('feet from curb',col('feet from curb').cast('int'))
	
# Filter out rows with null 'Law Section'   
df=df.filter(~col('Law Section').isNull())

# Filter out rows with specified values in 'Violation County'
values_to_drop = ['F', 'A', 'MS','P','K   F',108,'ABX'] 
df = df.filter(~col('Violation County').isin(values_to_drop))

# Drop rows with null values in specified columns having null values less than 5%
df =df.filter(~col('sub division').isNull())
df =df.filter(~col('plate id').isNull())
df =df.filter(~col('street name').isNull())

# Replace null values in 'vehicle make' with 'Not Mentioned'
df=df.withColumn('vehicle make',when(col('vehicle make').isNull(),'Not Mentioned').otherwise(col('vehicle make')))

# Replace 0 values in 'Violation Code' with 99
df =df.withColumn('Violation Code', when(col('violation code') == 0, 99).otherwise(col('Violation Code')))

df =df.withColumn('Vehicle Body Type', when(col('Vehicle Body Type').isNull(),'Others').otherwise(col('Vehicle Body Type')))


# Charting abbreviations against their whole forms for 'Violation County'
df = df.withColumn('Violation County',
                     when(df['Violation County'] == 'NY', 'Manhattan')
                     .when(df['Violation County'] == 'MN', 'Manhattan')
                     .when(df['Violation County'] == 'Q', 'Queens')
                     .when(df['Violation County'] == 'QN', 'Queens')
                     .when(df['Violation County'] == 'K', 'Brooklyn')
                     .when(df['Violation County'] == 'BK', 'Brooklyn')
                     .when(df['Violation County'] == 'ST', 'Richmond')
                     .when(df['Violation County'] == 'R', 'Richmond')
                     .when(df['Violation County'] == 'BX', 'Bronx')
                     .when(df['Violation County'] == 'QNS', 'Queens')
                     .when(df['Violation County'] == 'Qns', 'Queens')
                     .when(df['Violation County'] == 'QUEEN', 'Queens')
                     .when(df['Violation County'] == 'Rich', 'Richmond')
                     .when(df['Violation County'] == 'RICH', 'Richmond')
                     .when(df['Violation County'] == 'KINGS', 'Brooklyn')
                     .when(df['Violation County'] == 'Kings', 'Brooklyn')
                     .when(df['Violation County'] == 'BRONX', 'Bronx')
                     .when(df['Violation County'] == 'RICHM', 'Richmond')    
                     .otherwise(df['Violation County'])
                     )
			
# Charting abbreviations against their whole forms for 'Registration State'       
df = df.withColumn('Registration State',
                     when(df['Registration State'] == 'MN', 'Minnesota')
                     .when(df['Registration State'] == 'MX', 'Mexico')
                     .when(df['Registration State'] == 'DC', 'District of Columbia')
                     .when(df['Registration State'] == 'GV', 'Guanajuato (Mexico)')
                     .when(df['Registration State'] == 'QB', 'Queretaro (Mexico)')
                     .when(df['Registration State'] == 'MD', 'Maryland')
                     .when(df['Registration State'] == 'DE', 'Delaware')
                     .when(df['Registration State'] == 'MO', 'Missouri')
                     .when(df['Registration State'] == 'IL', 'Illinois')
                     .when(df['Registration State'] == 'MS', 'Mississippi')
                     .when(df['Registration State'] == 'SD', 'South Dakota')
                     .when(df['Registration State'] == 'ON', 'Ontario (Canada)')
                     .when(df['Registration State'] == 'AK', 'Alaska')
                     .when(df['Registration State'] == 'UT', 'Utah')
                     .when(df['Registration State'] == 'HI', 'Hawaii')
                     .when(df['Registration State'] == 'NS', 'Nova Scotia (Canada)')
                     .when(df['Registration State'] == 'CA', 'California')
                     .when(df['Registration State'] == 'CT', 'Connecticut')
                     .when(df['Registration State'] == 'NC', 'North Carolina')
                     .when(df['Registration State'] == 'ME', 'Maine')
                     .when(df['Registration State'] == 'NM', 'New Mexico')
                     .when(df['Registration State'] == 'IA', 'Iowa')
                     .when(df['Registration State'] == 'AR', 'Arkansas')
                     .when(df['Registration State'] == 'AZ', 'Arizona')
                     .when(df['Registration State'] == 'LA', 'Louisiana')
                     .when(df['Registration State'] == 'NJ', 'New Jersey')
                     .when(df['Registration State'] == 'NT', 'Northwest Territories (Canada)')
                     .when(df['Registration State'] == 'WI', 'Wisconsin')
                     .when(df['Registration State'] == 'MT', 'Montana')
                     .when(df['Registration State'] == 'MB', 'Manitoba (Canada)')
                     .when(df['Registration State'] == 'NY', 'New York')
                     .when(df['Registration State'] == 'FL', 'Florida')
                     .when(df['Registration State'] == 'PR', 'Puerto Rico')
                     .when(df['Registration State'] == 'KY', 'Kentucky')
                     .when(df['Registration State'] == 'WY', 'Wyoming')
                     .when(df['Registration State'] == 'BC', 'British Columbia (Canada)')
                     .when(df['Registration State'] == 'MI', 'Michigan')
                     .when(df['Registration State'] == 'NV', 'Nevada')
                     .when(df['Registration State'] == 'ID', 'Idaho')
                     .when(df['Registration State'] == 'VT', 'Vermont')
                     .when(df['Registration State'] == 'WA', 'Washington')
                     .when(df['Registration State'] == 'ND', 'North Dakota')
                     .when(df['Registration State'] == 'AL', 'Alabama')
                     .when(df['Registration State'] == 'IN', 'Indiana')
                     .when(df['Registration State'] == 'TN', 'Tennessee')
                     .when(df['Registration State'] == 'TX', 'Texas')
                     .when(df['Registration State'] == 'GA', 'Georgia')
                     .when(df['Registration State'] == 'CO', 'Colorado')
                     .when(df['Registration State'] == 'OK', 'Oklahoma')
                     .when(df['Registration State'] == 'SC', 'South Carolina')
                     .when(df['Registration State'] == 'OR', 'Oregon')
                     .when(df['Registration State'] == 'VA', 'Virginia')
                     .when(df['Registration State'] == 'RI', 'Rhode Island')
                     .when(df['Registration State'] == 'NH', 'New Hampshire')
                     .when(df['Registration State'] == 'NE', 'Nebraska')
                     .when(df['Registration State'] == 'OH', 'Ohio')
                     .when(df['Registration State'] == 'PA', 'Pennsylvania')
                     .when(df['Registration State'] == 'SK', 'Saskatchewan (Canada)')
                     .when(df['Registration State'] == 'AB', 'Alberta (Canada)')
                     .when(df['Registration State'] == 'PE', 'Prince Edward Island (Canada)')
                     .when(df['Registration State'] == 'WV', 'West Virginia')
                     .when(df['Registration State'] == 'MA', 'Massachusetts')
                     .when(df['Registration State'] == 'KS', 'Kansas')
                     .when(df['Registration State'] == 'NB', 'New Brunswick (Canada)')
                     .when(df['Registration State'] == 'YT', 'Yukon Territory (Canada)')
                     .when(df['Registration State'] == 'NF', 'Newfoundland (Canada)')
					 .when(df['Registration State'] == 'DP', 'U.S. State Dept')
					 .when(df['Registration State'] == 'FO', 'Foreign')
                    )

# Filter out rows with null 'Registration State' 
df=df.filter(~col('Registration State').isNull())

# Charting abbreviations against their whole forms for 'Issuing Agency'
df = df.withColumn('Issuing Agency',
                     when(df['Issuing Agency'] == 'M', 'TRANSIT AUTHORITY')
                     .when(df['Issuing Agency'] == 'U', 'PARKING CONTROL UNIT')
                     .when(df['Issuing Agency'] == 'D', 'DEPARTMENT OF BUSINESS SERVICES')
                     .when(df['Issuing Agency'] == 'B', 'TRIBOROUGH BRIDGE AND TUNNEL POLICE')
                     .when(df['Issuing Agency'] == 'S', 'DEPARTMENT OF SANITATION')
                     .when(df['Issuing Agency'] == 'T', 'TRAFFIC')
                     .when(df['Issuing Agency'] == 'Z', 'METRO NORTH RAILROAD POLICE')
                     .when(df['Issuing Agency'] == 'P', 'POLICE DEPARTMENT')
                     .when(df['Issuing Agency'] == 'H', 'HOUSING AUTHORITY')
                     .when(df['Issuing Agency'] == 'F', 'FIRE DEPARTMENT')
                     .when(df['Issuing Agency'] == 'V', 'DEPARTMENT OF TRANSPORTATION')
                     .when(df['Issuing Agency'] == 'O', 'NYS COURT OFFICERS')
                     .when(df['Issuing Agency'] == 'C', 'CON RAIL')
                     .when(df['Issuing Agency'] == 'X', 'OTHER/UNKNOWN AGENCIE')
                     .when(df['Issuing Agency'] == '1', 'NYS OFFICE OF MENTAL HEALTH POLICE')
                     .when(df['Issuing Agency'] == 'K', 'PARKS DEPARTMENT')
                     .when(df['Issuing Agency'] == 'E', 'BOARD OF ESTIMATE')
                     .when(df['Issuing Agency'] == 'A', 'PORT AUTHORITY')
                     .when(df['Issuing Agency'] == 'R', 'NYC TRANSIT AUTHORITY MANAGERS')
                     .when(df['Issuing Agency'] == 'N', 'NYS PARKS POLICE')
                     .when(df['Issuing Agency'] == 'Y', 'HEALTH AND HOSPITAL CORP. POLICE')
                     .when(df['Issuing Agency'] == 'L', 'LONG ISLAND RAILROAD')
                     .when(df['Issuing Agency'] == 'G', 'TAXI AND LIMOUSINE COMMISSION')
                     .when(df['Issuing Agency'] == '3', 'ROOSEVELT ISLAND SECURITY')
                     .when(df['Issuing Agency'] == 'J', 'AMTRAK RAILROAD POLICE')
                     .when(df['Issuing Agency'] == 'W', 'HEALTH DEPARTMENT POLICE')
                     .when(df['Issuing Agency'] == 'I', 'STATEN ISLAND RAPID TRANSIT POLICE')
                     .when(df['Issuing Agency'] == '8', 'SEA GATE ASSOCIATION POLICE')
                     .when(df['Issuing Agency'] == 'Q', 'DEPARTMENT OF CORRECTION')
                     .when(df['Issuing Agency'] == '4', 'WATERFRONT COMMISSION OF NY HARBOR')
                     .when(df['Issuing Agency'] == '5', 'SUNY MARITIME COLLEGE')
                     .when(df['Issuing Agency'] == '9', 'NYC OFFICE OF THE SHERIFF')
                    )
					
# Grouping Vehicle Body Type based on common aspects
heavy_vehicle_keywords = [
    "TRAC", "TRK","TRK.", "TANK", "FLAT", "DUMP", "TR/C", "WG", "SEMI", "TR/T", "TRL", "TK", "TRAIL", 
    "TRAI", "TRUC", "CARG", "CMIX", "T/CR", "TR/E", "CARR", "TR", "VAN", "TRUCK", "TOW", "TRACT", 
    "DLR", "TRLR", "BUS", "FIRE", "GARB", "REF", "LNCH", "REFG", "MOT", "U", "SNOW", "H/WH", 
    "BU", "M/H", "UHAU", "CHAS", "BULK", "CB", "LTRL", "PSVA", "B/V", "DOLL", "HEAVY", 
    "EQ", "RAC", "RL", "RACK", "REFR", "CAN", "COOL", "TC", "STOR", "W/CR", "FRUE", "STAK", 
    "FURN", "MOV", "STK", "COM", "W/VA", "STAB", "ELEV", "SCH", "RID", "LOADE", "BLD", "W/CA", 
    "B/GR", "BOOM", "O/SE", "SCAF", "CONCR", "GROU", "ROOF", "ICE", "B/F", "SAND", "RECR", "DROPS", 
    "PORTA", "EXCAV", "WOOD", "CONST", "FIR", "LIM", "LIFT", "CATER", "SCHB", "RIG", "DR", "W/DR", 
    "G/GR", "G/GS", "SPRI", "FLEET", "SPCI", "ME", "AIR", "TIRE", "DETEC", "SALT", "LOAD", "D/CA", 
    "D/CW", "INDU", "SCRA", "W/GA", "W/FO", "W/OI", "D/CE", "M/SK", 'BUS.', 'TRL.']

two_wheeler = ["MCY", "MC", "MOPD", "MOTO", "ATV", "SC", "SKEB", "TRIC", "MOP", "SCOO", "SCOT", "MOTOR", "MOPG", "MOPP", "MOPS", "MOPH", "MCY", "MCR", "SPO", "SPOF", "SPS", "SPSG", "SPSL", "SPV", "MCP", "TOM", "MRV", "MRVC", "MFW", "MB", "MRW", "OMS", "OTR", "OTRC", "OTH", "TO", "TOR", "TRS", "TSR", "TT", "TTM", "TTN", "TTP", "TTS", "TUBE", "TUK", "TURT", "TURU", "TUV", "TWIN", "TWOS", "TWTR"]

four_wheeler=['CXMI', 'TRUK', '2C', 'ZER', '4SLE', 'TRAL', 'ANXS', 'CT', 'ANB', 'DEL', 'OMR', 'AMG', 'HNH', 'WSR', 'VEH-', 'DELE', 'PICH', 'AMR', 'IX', 'TRY', 'GV', 'NC', 'MINI', 'UTIL', 'BL', 'OTHE', 'CV', 'SN/P', 'LIMO', 'TV', 'POWE', 'FOUR', 'CMHI', 'TRRL', 'HIWH', '5D', 'INCE', 'ET', 'KIA', 'ANL', 'JEEP', 'AO', 'ONAT', 'LMB', 'TRAILER', 'MX', 'BILR', 'TRAU', 'NXC', 'CKNE', 'CAM', 'STWA', 'SNL', 'AIAI', '2DSD', 'PAS', 'OU', 'CL', 'WORK', 'CONY', 'FGHT', 'CH', 'BMS', 'TWOD', 'ALL', 'HUMM', 'OLHE', 'BLUE', 'FRT', 'EXPL', 'HOUS', 'ORM', 'OLX', 'OTNE', 'AS', 'XRE', 'RF', 'BX', 'RJZL', '2HB', 'N/S', 'CXI', 'ANEL', 'ONRL', '4DSE', 'TU', 'HYUN', 'RANG', 'ANOE', 'MH', 'HY', 'TLE', 'OVX', 'PASS', 'MD', 'MN', 'CMRU', 'TRIM', 'ROS', 'CUSH', '4S', 'TERL', 'REG', 'SUBN', 'NS', 'PORS', 'NG', 'RPLC', 'TRAD', 'H/TR', 'EC', 'CAMI', 'TARI', 'C4', 'OVL', 'SP', 'EXPE', 'SPOR', 'AGI', 'NER', '2 DR', 'TBL', 'HC', 'P-U', 'RMA', 'UTLI', 'AM', 'MOC', 'TRLA', 'MO', 'AK', 'SHUT', 'STRA', 'UT', '4DSW', 'BOL', 'BUD', 'DEFI', 'MDEL', 'B', 'OSOO', 'APPP', 'CMR', 'GONV', 'REFG', 'GY', 'BOAT', '2HT', 'RRB', 'REK', 'IML', 'SRIB', 'ANOL', 'T', 'CW', 'TRUL', 'IMS', 'SWT', 'COPE', 'CA', '8V', 'TPAC', 'ISUZ', 'PICK', '4C', 'OVC', 'MOCL', 'REFU', 'CLLM', 'TAHO', 'SD', '2F', 'PICKU', 'SUW', 'MCL', 'MCTO', 'PU', 'LTR', 'W/SR', 'OMU', 'THR', 'BS', 'HRSE', 'M', 'OTM', 'CRLA', 'AEL', 'MACK', '2DSE', 'TLK', 'PATH', 'RD', 'MOPE', 'ULT', 'CONV', 'CRAF', 'OOCR', 'PRK', 'G', 'OVE', 'JOII', 'SAT', 'RAIL', 'TRF', 'MTCY', 'SV', 'OB', 'STAW', 'RTUC', 'ON', 'TAA', 'THW', 'MTCL', 'OUTB', 'FLTB', 'COOP', 'WAGON', 'OOL', 'GLAT', 'COUP', 'BIS', 'RCE', 'II', 'PCL', 'MR', 'C', 'HYNH', 'YW', 'ASRC', 'LUXV', 'TRHR', 'DELIVERY', 'TRAV', 'OORL', 'BK', 'BURG', 'FORD', 'SDN', 'CNIR', 'RXM', 'FODO', 'EGOL', 'SL', '2Z', 'RGR', 'ILI', 'RAD', 'LSUO', 'R', 'UV', 'TLC', 'MS', 'DCOM', 'TRCL', 'ORS', 'MERC', 'BN', 'MACY', 'C/CB', 'FREG', 'UITL', 'TS', 'TREA', 'PT', 'OUXL', '4DSD', 'TAXI', 'CXRG', '4 DR', 'CHRY', '4DRS', 'CI', 'H/IN', 'SRF', 'DOZ', 'MCYL', 'XXM', 'IDLL', 'S', 'VW', 'WAG', 'CZ', 'U', 'AMF', 'COHV', 'PICK-UP', 'ILSJ', 'UM', 'DELV', 'PCV', 'OSOL', 'RBM', 'TRKC', 'YK', 'HWO', 'SM', 'OML', 'WB', 'REFN', 'P/SH', 'OII', 'BOXT', 'YY', 'MOBL', '4DHT', 'PC', 'L', 'SUL', 'OMRL', 'PK', 'NOL', 'SCL', 'BOX', 'BLBI', 'ULIT', 'MEC', '4T', '4DHB', 'STAG', 'H/WH', 'CUST', 'LNCH', 'PINN', 'LO', 'REFF', 'D', 'RFEF', 'EO', '34PU', 'POLE', 'CNI', 'STAR', 'PICKUP', 'JUTI', '2CV', 'MOTC', 'PLSH', 'Four Wheeler', '4 WH', 'COU', 'DECA', 'ADP', 'CX', 'CMC', 'MODC', '4 DO', 'MP', 'IC', 'TKR', 'SS', 'TAX', 'WAGO', '12PU', 'MK', 'VHL', 'FLFT', 'TRD', 'HWH', 'AMB', 'ONE', 'LL', 'GRAN', 'OIM', 'HOW', 'R/RD', 'IJ', 'OLO', 'CM', 'SCHO', '3D', 'ARC', 'GOUR', 'APP', 'BLAC']

# Adding new column 'Vehicle Size' based on 'Vehicle Body Type'
df = df.withColumn('Vehicle Size',
                      when(df['Vehicle Body Type'].isin(heavy_vehicle_keywords), 'Heavy Vehicle')
                      .when(df['Vehicle Body Type'].isin(two_wheeler), 'Two Wheeler')
                      .when(df['Vehicle Body Type'].isin(four_wheeler), 'Four Wheeler')
                      .otherwise('Others'))

# Cleaning 'Violation Time'
df1=df.filter(~substring(col('Violation Time'),-1,1).isin('A','P'))

df1 = df1.withColumn("Violation Time",
                    concat(substring(col("Violation Time"), 1, 2), lit(":"), substring(col("Violation Time"), 3, 2)))

df2=df.filter(substring(col('Violation Time'),-1,1)=='A')	
	
df2 = df2.withColumn("Violation Time",
                    concat(substring(col("Violation Time"), 1, 2), lit(":"), substring(col("Violation Time"), 3, 2)))


df8=df.filter(substring(col('Violation Time'),-1,1)=='P')
df8=df8.filter(substring(col('violation time'),1,2)=='12')
df8 = df8.withColumn("Violation Time",
                    concat(substring(col("Violation Time"), 1, 2), lit(":"), substring(col("Violation Time"), 3, 2)))

df9=df.filter(substring(col('Violation Time'),-1,1)=='P')
df9=df9.filter(substring(col('violation time'),1,2)!='12')

df9 = df9.withColumn('Hour', when(substring('Violation Time', -1, 1) == 'P', 
                                     concat(substring('Violation Time', 1, 2)).cast("int") + 12)
                      .otherwise(substring('Violation Time', 1, 2)))
				
df9 = df9.withColumn('Violation Time', concat(col('Hour'), lit(":"), substring(col('Violation Time'), 3, 2)))

df9=df9.drop('Hour')
df3=df8.union(df9)

df4=df1.union(df2)
df=df4.union(df3)

df = df.filter(df['violation time'].rlike('^\\d{2}:\\d{2}$'))
df = df.withColumn("violation time", to_timestamp("violation time", "HH:mm"))

df = df.withColumn(
    "violation time",
    date_format("violation time", "HH:mm")
)

df = df.withColumn("violation time", to_timestamp(col("violation time"), "HH:mm"))

df =df.filter(~col('Violation Time').isNull())
# Charting abbreviations against their whole forms for 'Violation in front of or opposite'
df = df.withColumn('violation in front of or opposite',
                     when(col('violation in front of or opposite') == 'R', 'Right')
                     .when(col('violation in front of or opposite') == 'F', 'Front')
                     .when(col('violation in front of or opposite') == 'O', 'Opposite')
                     .when(col('violation in front of or opposite') == 'I', 'Intersection')
                     .otherwise('Others'))
		
# Mapping 'Plate Type'       
df = df.withColumn('Plate Type',
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
                     .otherwise('Others'))

# 'Vehicle Color' 					 
df = df.withColumn('Vehicle Color',
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

df = df.dropDuplicates()

df1=spark.read.parquet("s3://nycgroup05datalake/smalltable",header=True)

df2 = df.join(df1, df['violation code'] == df1['violation code'], how='inner')

df2=df2.select(df['*'],df1['Violation Description'],df1['Manhattan Fine'],df1['Other Fine'])

df3 = df2.withColumn('Fine Amount', when(df2['violation county'] == 'Manhattan', df2['Manhattan Fine']).otherwise(df2['Other Fine']))

df3=df3.drop('Manhattan Fine','Other Fine')

df3 = df3.withColumn("Fine Amount", col("Fine Amount").cast(LongType()))
	 
df3.coalesce(20).write \
	  .format("parquet") \
	  .option("header", "true") \
	  .mode("overwrite") \
	  .save("s3://nycgroup05datawarehouse/warehousenyc")
	  
	  
spark.stop()

