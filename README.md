<<<<<<< HEAD

MSDS696 Data Practicum II: Estimating Scope 3 Emissions at Scale

This project focuses on building scalable, interpretable models to estimate corporate Scope 3 greenhouse gas emissions using a combination of financial data, Scope 1 & 2 emissions, and company metadata reported to CDP from 2013–2023. After extensive data cleaning and integration—including backfilling missing metadata and standardizing emissions figures—various predictive modeling approaches were evaluated, including AdaBoost regressors and hierarchical linear models (HLMs) with sector- and company-level random effects. To address the influence of extreme outliers, the analysis employed z-score filtering, winsorization, and residual correction techniques.

The final models demonstrated the value of hierarchical structures and residual learning for improving accuracy, particularly in underrepresented cases. While most results were included in the final presentation, some follow-on work (e.g., modeling with winsorized data) was completed afterward and included here for completeness and future exploration.


Practicum II Data Prep.ipynb - designed for preprocessing corporate emissions and financial data in preparation for modeling Scope 3 greenhouse gas emissions. It includes data cleaning, log transformations, feature engineering, and the creation of grouped datasets based on key categorical attributes. The notebook sets the foundation for hierarchical and residual modeling workflows by structuring inputs and removing inconsistencies. Produces account_dictionary.json which can be used to fill in needed data before modeling.


account_dictionary.json -  used to fill in missing info in CDP data



Scope1_2_Analysis.ipynb - focused on extracting and aggregating Scope 1 and Scope 2 emissions data from CDP climate disclosure workbooks. It systematically reads in relevant worksheets, processes emissions figures, and calculates annual totals across all available years. The final output is stored as a structured JSON file (combined_scope_1_2.json) for use in downstream modeling or analysis. This notebook serves as a foundational step for incorporating direct emissions data into broader GHG forecasting efforts.

combined_scope_1_2.json -  used to fill in Scope 1&2 totals by account ID and year


Calculation_Methods_Analysis.ipynb - investigates the relationship between reported Scope 3 emissions Calculation Methods and the prevalence of extreme outliers in corporate emissions data. The notebook analyzes how different methodologies—such as spend-based, average data, and hybrid approaches—correlate with unusually high or inconsistent emissions values across companies and years. Through visualizations and statistical summaries, it highlights trends in data quality and potential methodological biases that may impact modeling accuracy. This analysis helps inform the treatment of calculation methods as a potential feature or filtering criterion in predictive emissions models.


Scope_3Data_Prep.ipynb - This notebook prepares corporate Scope 3 emissions data for predictive modeling by cleaning, filtering, and transforming reported values. It removes zero and null entries, applies log transformations to normalize skewed emissions distributions, and organizes data by Scope 3 category for downstream modeling. The script also sets up a consistent format for feature selection and training-ready datasets. This preprocessing stage supports more accurate and interpretable emissions prediction workflows.

Scope_1_2_kclusters.ipynb - This notebook applies K-means clustering to companies’ Scope 1 and 2 emissions data to identify potential patterns or groupings across firms. Z-score normalization is used to standardize the emissions data, and clusters are visualized interactively using Plotly for better interpretability. The goal was to uncover emission behavior similarities across industries or regions, though clear segmentation proved limited. Nonetheless, the analysis serves as an exploratory attempt to enhance understanding of Scope 1 and 2 reporting variability.

Final_Data_Prep.ipynb - This notebook finalizes the preparation of emissions and financial data for model training and evaluation. It merges preprocessed Scope 3 emissions with Scope 1 and 2 totals, financial indicators, and company metadata. Additional transformations ensure all numeric features are appropriately scaled and missing values are addressed for compatibility with machine learning pipelines. The resulting dataset is structured to support regression and hierarchical modeling of greenhouse gas emissions.


HLM_high_z_scores_filtered_data.ipynb - finalizes the preparation of emissions and financial data for model training and evaluation. It merges preprocessed Scope 3 emissions with Scope 1 and 2 totals, financial indicators, and company metadata. Additional transformations ensure all numeric features are appropriately scaled and missing values are addressed for compatibility with machine learning pipelines. The resulting dataset is structured to support regression and hierarchical modeling of greenhouse gas emissions.  This notebook contains the code and output for the highest metrics reported in the project presentation.


HLM_winsorized_data.ipynb - This notebook applies a hierarchical linear model (HLM) to predict Scope 3 emissions using winsorized data, incorporating financial and Scope 1 & 2 features. The analysis was completed after the project presentation, and as such, the results were not included in the original reported findings. It serves as a supplemental exploration to assess the robustness and potential improvements from using winsorized targets in hierarchical modeling.


Results_Comparison.ipynb - compares the performance of multiple predictive models for estimating Scope 3 greenhouse gas emissions. It evaluates models using metrics such as R², RMSE, RMSLE, and MAPE in both log-transformed and original metric ton scales. The analysis highlights the trade-offs between interpretability and accuracy, especially when handling outliers and extreme emission values. Visualizations and summary tables provide clear insights into each model’s strengths and limitations.

AdaBoost_GHG_filtered_z_data.ipynb - This notebook builds and evaluates an AdaBoost regression model to predict Scope 3 greenhouse gas emissions using filtered z-score standardized data. The analysis focuses on leveraging financial and Scope 1 & 2 emission features to estimate Scope 3 amounts, with preprocessing ensuring that only valid, standardized inputs are used. Model performance is assessed using metrics such as R², RMSE, and MAPE. This approach aims to improve prediction accuracy while reducing the influence of extreme outliers.

AdaBoost_filtered_z_data_Industry - This notebook explored an alternative modeling approach by restructuring the dataframes by Industry rather than Scope 3 Source Type. The rationale was that emission types might be more closely linked to industry characteristics. However, the resulting performance metrics were significantly lower than those from the original structure, so this approach was not pursued further."# MSDS696_Data_PracticumII" 

# MSDS696_Data_PracticumII
e026b9c16e24a17601760e14371a5f366bbeb5bb
