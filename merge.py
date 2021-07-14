import pandas as pd
data = pd.read_csv('owid-covid-data.csv')
data.drop('total_cases_per_million', inplace=True, axis=1)
data.drop('new_cases_per_million', inplace=True, axis=1)
data.drop('new_cases_smoothed_per_million', inplace=True, axis=1)
data.drop('total_deaths_per_million', inplace=True, axis=1)
data.drop('new_deaths_per_million', inplace=True, axis=1)
data.drop('new_deaths_smoothed_per_million', inplace=True, axis=1)
data.drop('reproduction_rate', inplace=True, axis=1)
data.drop('icu_patients', inplace=True, axis=1)
data.drop('hosp_patients', inplace=True, axis=1)
data.drop('icu_patients_per_million', inplace=True, axis=1)
data.drop('hosp_patients_per_million', inplace=True, axis=1)
data.drop('weekly_icu_admissions', inplace=True, axis=1)
data.drop('weekly_icu_admissions_per_million', inplace=True, axis=1)
data.drop('weekly_hosp_admissions', inplace=True, axis=1)
data.drop('weekly_hosp_admissions_per_million', inplace=True, axis=1)
data.drop('total_tests_per_thousand', inplace=True, axis=1)
data.drop('new_tests_per_thousand', inplace=True, axis=1)
data.drop('new_tests_smoothed', inplace=True, axis=1)
data.drop('new_tests_smoothed_per_thousand', inplace=True, axis=1)
data.drop('tests_units', inplace=True, axis=1)
data.drop('new_vaccinations_smoothed', inplace=True, axis=1)
data.drop('total_vaccinations_per_hundred', inplace=True, axis=1)
data.drop('people_vaccinated_per_hundred', inplace=True, axis=1)
data.drop('people_fully_vaccinated_per_hundred', inplace=True, axis=1)
data.drop('new_vaccinations_smoothed_per_million', inplace=True, axis=1)
data.drop('median_age', inplace=True, axis=1)
data.drop('aged_65_older', inplace=True, axis=1)
data.drop('aged_70_older', inplace=True, axis=1)
data.drop('gdp_per_capita', inplace=True, axis=1)
data.drop('extreme_poverty', inplace=True, axis=1)
data.drop('cardiovasc_death_rate', inplace=True, axis=1)
data.drop('diabetes_prevalence', inplace=True, axis=1)
data.drop('female_smokers', inplace=True, axis=1)
data.drop('male_smokers', inplace=True, axis=1)
data.drop('handwashing_facilities', inplace=True, axis=1)
data.drop('hospital_beds_per_thousand', inplace=True, axis=1)
data.drop('life_expectancy', inplace=True, axis=1)
data.drop('human_development_index', inplace=True, axis=1)
data.drop('excess_mortality', inplace=True, axis=1)
data.drop('population_density', inplace=True, axis=1)
data.drop('population', inplace=True, axis=1)
data.drop('stringency_index', inplace=True, axis=1)
data.drop('iso_code', inplace=True, axis=1)
data.drop('continent', inplace=True, axis=1)

data.to_csv("cases_vaccine_merge.csv", index=None)

india_data = data[data["location"] == 'India']
india_data.drop('location', inplace=True, axis=1)

india_data.to_csv("processes_india_cases_vaccine_merge.csv",index=None)


us_data = data[data["location"] == 'United States']
us_data.drop('location', inplace=True, axis=1)

us_data.to_csv("processes_us_cases_vaccine_merge.csv",index=None)