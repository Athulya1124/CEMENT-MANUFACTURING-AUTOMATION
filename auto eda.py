
import pandas as pd
df=pd.read_csv(r"C:\Users\athuu\OneDrive\Desktop\Dataset (1).csv")
pip install sweetviz
import sweetviz as sv
analyze_report=sv.analyze(df)
analyze_report.show_html('report.html')
