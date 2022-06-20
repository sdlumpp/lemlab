import lemlab

if __name__ == "__main__":
    seed = 14
    sim_name = f"case_study_4_1"

    analysis = lemlab.ScenarioAnalyzer(path_results=f"./simulation_results/{sim_name}",
                                       show_figures=True,
                                       save_figures=True,
                                       path_plotstyle="../../../lemlab/lemlab_plots.mplstyle")
    analysis.run_analysis()
