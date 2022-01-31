from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
import pandas as pd
import os


def transform():
    pd.set_option('display.max_columns', None)

    df = pd.read_excel('/opt/airflow/dags/data_source/cgi40/76459_GCR%2017-19%20Dataset.xlsx',
                       sheet_name='Data', skiprows=2, engine="openpyxl").drop(0)

    i = 3
    df = pd.concat([df.iloc[:, :i],
                    pd.DataFrame('',
                                 columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                          'sub_indicator', 'others'],
                                 index=df.index), df.iloc[:, i:]],
                   axis=1)

    df.rename(columns={'Edition': 'year', 'Index': 'index',
              'Country': 'country'}, inplace=True)
    df.drop(['Series code (if applicable)', 'Series name'], axis=1, inplace=True)
    df = df[df['Attribute'] != 'PERIOD']

    series_dict = {
        "EOSQ035": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Security",
            "indicator": "Organized crime (S)"
        },
        "HOMICIDERT": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Security",
            "indicator": "Homicide rate"
        },
        "TERRORISMINCIDENCEIDXGCI4": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Security",
            "indicator": "Terrorism incidence"
        },
        "EOSQ055": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Security",
            "indicator": "Reliability of police services (S)"
        },
        "GCI4.A.01.01": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Security",
        },
        "LEGATPISOCI": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Social capital",
            "indicator": "Social capital"
        },
        "GCI4.A.01.02": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Social capital",
        },
        "OPENBUDGETIDX": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Checks and balances",
            "indicator": "Budget transparency"
        },
        "EOSQ144": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Checks and balances",
            "indicator": "Judicial independence (S)"
        },
        "FREEPRESSRWB": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Checks and balances",
            "indicator": "Freedom of the press"
        },
        "GCI4.A.01.03": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Checks and balances",
        },
        "EOSQ048": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Public-sector performance",
            "indicator": "Burden of government regulation"
        },
        "EOSQ040": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Public-sector performance",
            "indicator": "Efficiency of legal framework in settling disputes"
        },
        "UNPANEPARTIDX": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Public-sector performance",
            "indicator": "E-Participation"
        },
        "GCI4.A.01.04": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Public-sector performance",
        },
        "TRANSPARENCYCPI": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Transparency",
            "indicator": "Incidence of corruption"
        },
        "GCI4.A.01.05": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Transparency",
        },
        "EOSQ051": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Property rights",
            "indicator": "Property rights"
        },
        "EOSQ052": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Property rights",
            "indicator": "Intellectual property protection"
        },
        "DBREGPROPADMINQUAL": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Property rights",
            "indicator": "Quality of land administration"
        },
        "GCI4.A.01.06": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Property rights",
        },
        "EOSQ097": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Corporate governance",
            "indicator": "Strength of auditing and accounting standards"
        },
        "CONFINTREG": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Corporate governance",
            "indicator": "Conflict of interest regulation"
        },
        "SHRHOLDGOV": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Corporate governance",
            "indicator": "Shareholder governance"
        },
        "GCI4.A.01.07": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Corporate governance",
        },
        "EOSQ434": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Government adaptability",
            "indicator": "Government ensuring policy stability"
        },
        "EOSQ507": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Government adaptability",
            "indicator": "Government's responsiveness to change"
        },
        "EOSQ509": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Government adaptability",
            "indicator": "Legal framework's adaptability to digital business models"
        },
        "EOSQ510": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Government adaptability",
            "indicator": "Government long-term vision"
        },
        "GOVADAPT": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Government adaptability",
        },
        "ENVTREATY": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Commitment to sustainability",
            "indicator": "Environment-related treaties in force"
        },
        "RISEEFF": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Commitment to sustainability",
            "indicator": "Energy efficiency regulation"
        },
        "RISERES": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Commitment to sustainability",
            "indicator": "Renewable energy regulation"
        },
        "SUSTCOMMIT": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
            "sub_sub_pillar": "Commitment to sustainability",
        },
        "GCI4.A.01.08": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
        },
        "GCI4.C.10": {
            "pillar": "10th pillar: Market size",
        },
        "ROADQUALIDX": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Road",
            "indicator": "Road connectivity"
        },
        "EOSQ057": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Road",
            "indicator": "Quality of road infrastructure"
        },
        "ROADINF": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Road",
        },
        "RAILDENS": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Railroad",
            "indicator": "Railroad density"
        },
        "EOSQ485": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Railroad",
            "indicator": "Efficiency of train services"
        },
        "RAILINF": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Railroad",
        },
        "IATACONNECTIDX": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Air",
            "indicator": "Airport connectivity"
        },
        "EOSQ486": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Air",
            "indicator": "Efficiency of air transport services"
        },
        "AIRINF": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Air",
        },
        "LINERSHIPIDX": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Sea",
            "indicator": "Liner shipping connectivity"
        },
        "EOSQ487": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Sea",
            "indicator": "Efficiency of seaport services"
        },
        "SEAINF": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
            "sub_sub_pillar": "Sea",
        },
        "GCI4.A.02.01": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Transport infrastructure",
        },
        "ELECRATE": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
            "sub_sub_pillar": "Electricity",
            "indicator": "Electricity access"
        },
        "POWERLOS": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
            "sub_sub_pillar": "Electricity",
            "indicator": "Electricity supply quality"
        },
        "ELECINF": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
            "sub_sub_pillar": "Electricity",
        },
        "UNSAFEWATEREXPO": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
            "sub_sub_pillar": "Water",
            "indicator": "Exposure to unsafe drinking water"
        },
        "EOSQ488": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
            "sub_sub_pillar": "Water",
            "indicator": "Reliability of water supply"
        },
        "WATERINF": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
            "sub_sub_pillar": "Water",
        },
        "GCI4.A.02.02": {
            "pillar": "2nd pillar: Infrastructure",
            "sub_pillar": "Utility infrastructure",
        },
        "EOSQ039": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Public-sector performance",
            "indicator": "Efficiency of legal framework in settling disputes"
        },
        "MOBSUBPC": {
            "pillar": "3rd pillar: ICT adoption",
            "indicator": "Mobile-cellular telephone subscriptions"
        },
        "MOBBBSUBPC": {
            "pillar": "3rd pillar: ICT adoption",
            "indicator": "Mobile-broadband subscriptions"
        },
        "BBSUBPC": {
            "pillar": "3rd pillar: ICT adoption",
            "indicator": "Fixed-broadband Internet subscriptions"
        },
        "OPTICFIBRESUBSPC": {
            "pillar": "3rd pillar: ICT adoption",
            "indicator": "Fibre internet subscriptions"
        },
        "NETUSERPCT": {
            "pillar": "3rd pillar: ICT adoption",
            "indicator": "Internet users"
        },
        "CURRWORKEDUQUALITY": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
        },
        "INFLAYRAVG": {
            "pillar": "4th pillar: Macroeconomic stability",
            "indicator": "Inflation"
        },
        "DEBDYNM": {
            "pillar": "4th pillar: Macroeconomic stability",
            "indicator": "Debt dynamics"
        },
        "GOVDEBTGDP": {
            "pillar": "4th pillar: Macroeconomic stability",
            "indicator": "Government debt to GDP"
        },
        "GOVDEBTGDPCHANGE": {
            "pillar": "4th pillar: Macroeconomic stability",
            "indicator": "Change in government debt"
        },
        "CREDITRATINGGRADE": {
            "pillar": "4th pillar: Macroeconomic stability",
            "indicator": "Country bond ratings"
        },
        "GCI4.D.11": {
            "pillar": "11th pillar: Business dynamism",
        },
        "GCI4.SUBIDXA": {
            "pillar": "1st pillar: Institutions",
        },
        "HEALTHYLIFEXP": {
            "pillar": "5th pillar: Health",
            "indicator": "Healthy life expectancy"
        },
        "MYSCHOOLALL": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Education of current workforce",
            "indicator": "Mean years of schooling"
        },
        "EOSQ139": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Extent of staff training"
        },
        "EOSQ436": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Quality of vocational training"
        },
        "EOSQ403": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Ease of finding skilled employees"
        },
        "EOSQ508": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Digital skills among active population"
        },
        "EOSQ495": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Skillset of secondary-education graduates"
        },
        "EOSQ496": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Skillset of university graduates"
        },
        "GRADSKILLS": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
            "sub_sub_pillar": "Skills of current workforce",
            "indicator": "Skillset of graduates"
        },
        "GCI4.B.06.01": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Current workforce",
        },
        "SCHLIFEXPALL": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Future workforce",
            "sub_sub_pillar": "Education of future workforce",
            "indicator": "School life expectancy"
        },
        "PUPTEACHRATIO1": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Future workforce",
            "sub_sub_pillar": "Skills of future workforce",
            "indicator": "Pupil-to-teacher ratio in primary education"
        },
        "EOSQ498": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Future workforce",
            "sub_sub_pillar": "Skills of future workforce",
            "indicator": "Critical thinking in teaching"
        },
        "GCI4.B.06.02": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Future workforce",
            "sub_sub_pillar": "Skills of future workforce",
        },
        "GCI4.SUBIDXB": {
            "sub_index": "Human capital"
        },
        "EOSQ105": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
            "indicator": "Extent of market dominance",
        },
        "EOSQ045": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
            "indicator": "Distortive effect of taxes and subsidies on competition",
        },
        "EOSQ489": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
            "indicator": "Competition in services",
            "sub_indicator": "Competition in professional services"
        },
        "EOSQ490": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
            "indicator": "Competition in services",
            "sub_indicator": "Competition in retail services"
        },
        "EOSQ491": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
            "indicator": "Competition in services",
            "sub_indicator": "Competition in network services"
        },
        "SERVCOMP": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
            "indicator": "Competition in services",
        },
        "GCI4.C.07.01": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Domestic competition",
        },
        "EOSQ096": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Trade openness",
            "indicator": "Prevalence of non-tariff barriers",
        },
        "TFDUTY": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Trade openness",
            "indicator": "Trade tariffs",
        },
        "TARIFCOMPL": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Trade openness",
            "indicator": "Complexity of tariffs",
        },
        "CLEAREFF": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Trade openness",
            "indicator": "Border clearance efficiency",
        },
        "GCI4.C.07.02": {
            "pillar": "7th pillar: Product market",
            "sub_pillar": "Trade openness",
        },
        "GCI4.D.12": {
            "pillar": "12th pillar: Innovation capability",
        },
        "REDUNCOST": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Redundancy costs",
        },
        "EOSQ134": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Hiring and firing practices",
        },
        "EOSQ135": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Cooperation in labour-employer relations",
        },
        "EOSQ136": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Flexibility of wage determination",
        },
        "EOSQ497": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Active labour market policies",
        },
        "WORKRIGHT": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Workers' rights",
        },
        "EOSQ138": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Ease of hiring foreign labour",
        },
        "EOSQ499": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
            "indicator": "Internal labour mobility",
        },
        "GCI4.C.08.01": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Flexibility",
        },
        "EOSQ126": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Meritocracy and incentivization",
            "indicator": "Reliance on professional management",
        },
        "EOSQ137": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Meritocracy and incentivization",
            "indicator": "Pay and productivity",
        },
        "FMLWGEDWRKRT": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Meritocracy and incentivization",
            "indicator": "Ratio of wage and salaried female workers to male workers",
        },
        "LABTAX": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Meritocracy and incentivization",
            "indicator": "Labour tax rate",
        },
        "GCI4.C.08.02": {
            "pillar": "8th pillar: Labour market",
            "sub_pillar": "Meritocracy and incentivization",
        },
        "FUTURWORKEDUQUALITY": {
            "pillar": "6th pillar: Skills",
            "sub_pillar": "Future workforce",
            "sub_sub_pillar": "Skills of future workforce",
        },
        "DOMCREDITGDP": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Domestic credit to private sector",
        },
        "EOSQ425": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Financing of SMEs",
        },
        "EOSQ089": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Venture capital availability",
        },
        "LIFEINSURPREMGDP": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Venture capital availability",
            "sub_indicator": "Life insurance premiums"
        },
        "NLIFEINSURPREMGDP": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Venture capital availability",
            "sub_indicator": "Non-life insurance premiums"
        },
        "INSURPREMGDP": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Venture capital availability",
            "sub_indicator": "Life and non-life insurance premium"
        },
        "MKTCAP": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Market capitalization",
        },
        "GCI4.C.09.01": {
            "pillar": "9th pillar: Financial system",
            "sub_index": "Market capitalization",
        },
        "EOSQ087": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Soundness of banks",
        },
        "CREDGDPGAP": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Credit gap",
        },
        "NPLOANS": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Non-performing loans",
        },
        "BANKCAPRWA": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Banks’ regulatory capital ratio",
        },
        "GCI4.C.09.02": {
            "pillar": "9th pillar: Financial system",
            "indicator": "Stability",
        },
        "GDPPPPC": {
            "pillar": "10th pillar: Market size",
            "indicator": "Gross domestic product",
        },
        "IMPGDP": {
            "pillar": "10th pillar: Market size",
            "indicator": "Gross domestic product",
        },
        "GCI4.A.01": {
            "pillar": "1st pillar: Institutions",
        },
        "GCI4.SUBIDXC": {
            "sub_index": "Markets",
        },
        "STARTBUSCOST": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Administrative requirements",
            "indicator": "Cost of starting a business",
        },
        "STARTBUSDAYS": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Administrative requirements",
            "indicator": "Time to start a business",
        },
        "INSOLVENCYRECOVERYRATE": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Administrative requirements",
            "indicator": "Insolvency recovery rate",
        },
        "INSOLVFRAME": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Administrative requirements",
            "indicator": "Insolvency regulatory framework",
        },
        "GCI4.D.11.01": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Administrative requirements",
        },
        "EOSQ073": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Entrepreneurial culture",
            "indicator": "Attitudes towards entrepreneurial risk",
        },
        "EOSQ362": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Entrepreneurial culture",
            "indicator": "Growth of innovative companies",
        },
        "EOSQ470": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Entrepreneurial culture",
            "indicator": "Willingness to delegate authority",
        },
        "EOSQ432": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Entrepreneurial culture",
            "indicator": "Companies embracing disruptive ideas",
        },
        "GCI4.D.11.02": {
            "pillar": "11th pillar: Business dynamism",
            "sub_pillar": "Entrepreneurial culture",
        },
        "EOSQ429": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Multistakeholder collaboration",
            "sub_indicator": "Collaboration within a company",
        },
        "EOSQ493": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Multistakeholder collaboration",
            "sub_indicator": "Collaboration between companies",
        },
        "EOSQ072": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Multistakeholder collaboration",
            "sub_indicator": "University-industry collaboration in R&D",
        },
        "MULTISTAKECOLLAB": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Multistakeholder collaboration",
        },
        "EOSQ109": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "State of cluster development",
        },
        "IP5INTLCOINVPOP": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "International Patent applications",
        },
        "EOSQ505": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Diversity of workforce",
        },
        "SMGHIDEX": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Scientific publications",
        },
        "RESINSTPROMIDX": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Research institutions prominence",
        },
        "IP5PATPOP": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Patent applications",
        },
        "RDSPENDING": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "R&D expenditures",
        },
        "RESEARCH": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Research and development",
        },
        "EOSQ100": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Buyer sophistication",
        },
        "TRADEMARK": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Trademark applications",
        },
        "INVCOMMERC": {
            "pillar": "12th pillar: Innovation capability",
            "indicator": "Commercialization",
        },
        "GCI4.SUBIDXD": {
            "sub_index": "Innovation ecosystem",
        },
        "EXPIDEAS": {
            "others": "Interaction and diversity",
        },
        "GCI4.C.09": {
            "pillar": "9th pillar: Financial system",
        },
        "GCI4.C.08": {
            "pillar": "8th pillar: Labour market",
        },
        "GCI4.C.07": {
            "pillar": "7th pillar: Product market",
        },
        "SRVCTRADERESTRICT": {
            "others": "Service Trade Restrictiveness Index",
        },
        "RAILROADBIN": {
            "others": "Railroad existence",
        },
        "OPTICRATIO": {
            "others": "Ratio fibre subs to fixed-broadband subs using min (subs;50)",
        },
        "LANDLOCKEDBIN": {
            "others": "Landlocked economy (1=Yes;0=No)",
        },
        "MOBRATIO": {
            "others": "Ratio mobile broadband to total mobile subs using min(subs;120)",
        },
        "CITYSTATEBIN": {
            "others": "City state dummy (0=No; 1=Yes)",
        },
        "GDPPC": {
            "others": "GDP per capita (US$)",
        },
        "GROUP_CONCA": {
            "others": "Group belonging",
        },
        "OPENBUDGET": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Checks and balances",
            "indicator": "Budget transparency"
        },
        "FUTUREGOV": {
            "pillar": "1st pillar: Institutions",
            "sub_pillar": "Future orientation of government",
        },
        "GCI4.A.02": {
            "pillar": "2nd pillar: Infrastructure",
        },
        "GCI4.A.03": {
            "pillar": "3rd pillar: ICT adoption",
        },
        "GCI4.A.04": {
            "pillar": "4th pillar: Macroeconomic stability",
        },
        "GCI4.B.05": {
            "pillar": "5th pillar: Health",
        },
        "GCI4.B.06": {
            "pillar": "6th pillar: Skills",
        },
        "GCI4": {
            "others": "Global Competitiveness Index 4.0",
        },
    }

    def get_value_from_key(key, sub_key):
        return series_dict.get(key, {}).get(sub_key, "")

    df["pillar"] = df["Series Global ID"].apply(
        get_value_from_key, args=("pillar",))
    df["sub_pillar"] = df["Series Global ID"].apply(
        get_value_from_key, args=("sub_pillar",))
    df["sub_sub_pillar"] = df["Series Global ID"].apply(
        get_value_from_key, args=("sub_pillar_pillar",))
    df["indicator"] = df["Series Global ID"].apply(
        get_value_from_key, args=("indicator",))
    df["sub_index"] = df["Series Global ID"].apply(
        get_value_from_key, args=("sub_index",))
    df["sub_indicator"] = df["Series Global ID"].apply(
        get_value_from_key, args=("sub_indicator",))
    df["others"] = df["Series Global ID"].apply(
        get_value_from_key, args=("others",))

    df.drop('Series Global ID', axis=1, inplace=True)

    df = df.melt(
        id_vars=['index', 'year', 'sub_index', 'pillar', 'sub_pillar',
                 'sub_sub_pillar', 'indicator', 'sub_indicator', 'others', 'Attribute'],
        var_name="country",
        value_name="value"
    )

    ingest_date = datetime.now()

    df['organizer'] = 'WEF'
    df['master_index'] = 'CGI 4.0'
    df['date_etl'] = ingest_date.strftime("%Y-%m-%d %H:%M:%S")
    df.rename(columns={'Attribute': 'unit_2',
              'Dataset': 'index'}, inplace=True)

    col = ["country", "year", "master_index", "organizer", "index", "sub_index", "pillar", "sub_pillar", "sub_sub_pillar",
           "indicator", "sub_indicator", "others", "unit_2", "value", "date_etl"]

    df = df[col]
    year_list = df['year'].unique()

    for year in year_list:
        final = df[df['year'] == year].copy()
        current_year = str(year)[0:4]
        final['year'] = current_year

        final.to_csv('/opt/airflow/dags/output/cgi40/CGI_40_{}_{}.csv'.format(
            current_year, ingest_date.strftime("%Y%m%d%H%M%S")), index=False)


default_args = {
    'owner': 'ETDA',
    'depends_on_past': False,
    'start_date': '2021-01-25',
    'email': ['oulespaul@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@yearly',
}

dag = DAG('cgi_40', default_args=default_args, catchup=False)


def store_to_hdfs():
    hdfs = PyWebHdfsClient(host='10.121.101.145',
                           port='50070', user_name='cloudera')
    my_dir = '/user/cloudera/raw/index_dashboard/Global/CGI_4.0'
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/cgi40"

    os.chdir(path)

    for file in os.listdir():
        if file.endswith(".csv"):
            file_path = f"{path}/{file}"

            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+file, my_data.encode('utf-8'), overwrite=True)

                pprint("Stored! file: {}".format(file))
                pprint(hdfs.list_dir(my_dir))


with dag:
    ingestion = BashOperator(
        task_id='ingestion',
        bash_command='cd /opt/airflow/dags/data_source/cgi40 &&  curl -LfO "https://www.teknologisk.dk/_/media/76459_GCR%2017-19%20Dataset.xlsx"',
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
    )

ingestion >> transform >> load_to_hdfs
