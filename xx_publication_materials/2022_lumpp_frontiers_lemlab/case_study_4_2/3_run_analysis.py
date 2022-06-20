import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ruamel.yaml import YAML
import lemlab.db_connection.db_param as db_p
from lemlab.db_connection.db_connection import DatabaseConnection


class ScenarioAnalyzer:

    def __init__(self, path_results):

        self.path_results = path_results
        self.path_analyzer = f"{self.path_results}/analyzer"
        self.yaml = YAML()

        with open(f"{path_results}/config.yaml") as config_file:
            config = YAML().load(config_file)
        self.db_conn = DatabaseConnection(db_dict=config['db_connections']['database_connection_admin'],
                                          lem_config=config['lem'])

        self.max_time = self.__max_timestamp()
        self.conv_to_kWh = 1/1000           # to convert from Wh to kWh
        self.conv_to_EUR = 1/1000000000     # to convert from sigma to €

    def calculate_average_energy_cost(self) -> None:
        """plots the specific energy costs for each type of participant

        Args:
            all_types: boolean that specifies if plot should show also the non-existing types as column or not

        Returns:
            None

        """

        # Create dataframe to gather all the information and do the necessary calculations
        df_info = pd.DataFrame(columns=[db_p.ID_USER, db_p.ID_METER, "PV_Bat_EV_HP_Wind_Fix", "energy_sold_kWh",
                                        "revenue_sold_€", "energy_bought_kWh", "cost_bought_€", "energy_balance_kWh",
                                        "balance_€", "consumption_kWh", "avg_price_€/kWh"]).set_index(db_p.ID_USER)

        # Look up each participants main meter id
        df_meters = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0,
                                dtype={"id_user": str})
        df_temp = df_meters[df_meters[db_p.TYPE_METER].isin(["grid meter", "virtual grid meter"])] \
            [[db_p.ID_USER, db_p.ID_METER]]
        df_temp.set_index(db_p.ID_USER, inplace=True)
        df_info[db_p.ID_USER] = df_temp.index
        df_info.set_index(db_p.ID_USER, inplace=True)
        df_info[db_p.ID_METER] = df_temp

        # Sort all the transactions according to the user for the time period max_time
        df_transactions = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_LOGS_TRANSACTIONS}.csv",
                                      index_col=0)
        df_transactions = df_transactions[df_transactions[db_p.TS_DELIVERY] <= self.max_time]
        df_transactions = df_transactions[df_transactions[db_p.TS_DELIVERY] >= 1616540400]
        df_temp_pos = df_transactions[df_transactions[db_p.QTY_ENERGY] >= 0]
        df_temp_pos = df_temp_pos.groupby(db_p.ID_USER).sum()
        df_info["energy_sold_kWh"] = df_temp_pos[db_p.QTY_ENERGY] * self.conv_to_kWh
        df_info["revenue_sold_€"] = df_temp_pos[db_p.DELTA_BALANCE] * self.conv_to_EUR
        df_temp_neg = df_transactions[df_transactions[db_p.QTY_ENERGY] < 0]
        df_temp_neg_energy = df_temp_neg[df_temp_neg[db_p.TYPE_TRANSACTION].isin(["market", "balancing"])]. \
            groupby(db_p.ID_USER).sum()
        df_info["energy_bought_kWh"] = - df_temp_neg_energy[db_p.QTY_ENERGY] * self.conv_to_kWh
        df_info["cost_bought_€"] = - df_temp_neg_energy[db_p.DELTA_BALANCE] * self.conv_to_EUR

        df_info = df_info.fillna(0)
        df_info["energy_balance_kWh"] = df_info["energy_sold_kWh"] - df_info["energy_bought_kWh"]
        df_info["balance_€"] = df_info["revenue_sold_€"] - df_info["cost_bought_€"]
        df_info["n_participants"] = 1
        # Get the power consumption of every household by checking the submeter's delta readings
        df_consumption = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv",
                                     index_col=0)
        df_consumption = df_consumption[df_consumption[db_p.TS_DELIVERY] <= self.max_time]
        df_consumption = df_consumption[df_consumption[db_p.TS_DELIVERY] >= 1616540400]
        df_meters = df_meters.set_index("id_meter")
        df_consumption = df_consumption[df_consumption[db_p.TS_DELIVERY] <= self.max_time].sort_values(db_p.TS_DELIVERY)
        df_consumption = df_consumption.groupby("id_meter").sum()
        df_consumption["main_meter"] = df_meters["id_meter_super"]
        df_consumption["id_user"] = df_meters["id_user"]
        # Sort out all main meter, and PV, wind and fixed gen meters as only the consumption of the household load,
        # the heatpump, the ev the battery is important (current method simply adds up energy_in)
        df_consumption = df_consumption[df_consumption["main_meter"] != "0000000000"]  # delete main meter
        df_consumption = df_consumption[df_consumption["energy_in"] > 0]  # delete all generators
        df_consumption["energy_consumed"] = df_consumption["energy_in"]  # "- df_consumption["energy_out"]",
        #     if battery discharge to be subtracted
        df_consumption = df_consumption.groupby("id_user").sum()
        df_info["consumption_kWh"] = df_consumption["energy_consumed"] / 1000
        # avg_price: negative balance means positive price  (you have to pay for every kWh);
        # positive balance means negative price (you receive money for every kWh)
        df_info["avg_price_€/kWh"] = - df_info["balance_€"].abs() / df_info["consumption_kWh"]
        df_info.loc[abs(df_info["avg_price_€/kWh"]) == float("inf"), "avg_price_€/kWh"] = 0
        return df_info

    def __max_timestamp(self) -> int:
        """checks for the last cleared timestamp

        Args:

        Returns:
            integer that represents the last timestamp that was cleared

        """

        db_settlement_status = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_STATUS_SETTLEMENT}.csv",
                                           index_col=0)

        db_settlement_status = db_settlement_status.sort_values(by=db_p.TS_DELIVERY)

        return max(db_settlement_status[db_settlement_status[db_p.STATUS_SETTLEMENT_COMPLETE] == 1][db_p.TS_DELIVERY])


def run_analysis(_base_name, _prosumer, _range_trading_horizon, _range_time_shift):
    list_results = [[0 for _ in range_time_shift] for _ in range_trading_horizon]

    for ix_hor, trading_horizon in enumerate(range_trading_horizon):
        for ix_pr, time_shift in enumerate(range_time_shift):
            path_sim = f"./simulation_results/{_base_name}_pro_{_prosumer}_hor_{trading_horizon}_t_shift_{time_shift}"

            analysis = ScenarioAnalyzer(path_results=path_sim)

            user_avg_price = analysis.calculate_average_energy_cost().loc[str(prosumer).zfill(10), "avg_price_€/kWh"]

            list_results[ix_hor][ix_pr] = round(float(-1 * user_avg_price), 6) * 100

    plotting_data = {"title": f'Average price of energy on the LEM for prosumer {_prosumer}',
                     "y-label": "Trading horizon (h)",
                     "x-label": "Load time shift (h)",
                     "colorbar-label": "Average price(ct/kWh)",
                     "xticklabels": [x//4 for x in range_time_shift],
                     "yticklabels": [x//4 for x in range_trading_horizon],
                     "data": list_results, }

    with open(f"results_pro_{str(prosumer).zfill(10)}.yaml", 'w') as write_file:
        yaml = YAML()
        yaml.default_flow_style = True
        yaml.dump(plotting_data, write_file)


if __name__ == "__main__":
    base_name = f"case_study_4_2"

    list_prosumer = [1, 8]
    range_trading_horizon = range(1 * 4, 24 * 4, 2 * 4)
    range_time_shift = range(-18 * 4, 21 * 4, 3 * 4)

    for prosumer in list_prosumer:

        # run_analysis(_base_name=base_name,
        #              _prosumer=prosumer,
        #              _range_trading_horizon=range_trading_horizon,
        #              _range_time_shift=range_time_shift)

        with open(f"results_pro_{str(prosumer).zfill(10)}.yaml") as config_file:
            plot_config = YAML().load(config_file)

        ax = sns.heatmap(plot_config["data"],
                         linewidth=0.5,
                         cmap="rocket_r",
                         xticklabels=plot_config["xticklabels"],
                         yticklabels=plot_config["yticklabels"],
                         cbar_kws={'label': plot_config["colorbar-label"]},
                         annot=True,
                         annot_kws={"size": 9},
                         vmin=5, vmax=7.1,
                         )

        ax.set_xlabel(plot_config["x-label"])
        ax.set_ylabel(plot_config["y-label"])
        # ax.set_title(config["title"] + f"{prosumer}")
        plt.show()

        # make plot for demonstrating time shifts
        for time_shift in [0 * 4, 3 * 4, 6 * 4]:
            path_results = f"./simulation_results/{base_name}_pro_{prosumer}_hor_4_t_shift_{time_shift}"

            df_meters = pd.read_csv(f"{path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", dtype={"id_user": str})

            id_meter_hh = str(df_meters[(df_meters["id_user"] == str(prosumer).zfill(10))
                                         & (df_meters["info_additional"] == "residual load")]["id_meter"].iloc[0])

            df_readings_meter_delta = pd.read_csv(f"{path_results}/db_snapshot/{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv")
            df_readings_meter_delta = df_readings_meter_delta[df_readings_meter_delta[db_p.TS_DELIVERY] >= 1616540400]
            df_readings_meter_delta["ts_delivery"] = pd.to_datetime(df_readings_meter_delta["ts_delivery"], unit='s')
            ts_d_first, ts_d_last = min(df_readings_meter_delta), max(df_readings_meter_delta)

            df_readings_meter_delta.set_index("ts_delivery", inplace=True)

            ts_hh = df_readings_meter_delta[df_readings_meter_delta["id_meter"] == id_meter_hh].copy()
            ts_hh['energy_net'] = ts_hh['energy_out'] - ts_hh['energy_in']

            if time_shift == 0:
                plt.plot(ts_hh["energy_net"],
                         label="Base time series",
                         linewidth=1.2,
                         color="black")
                plt.xticks(rotation=30)
            elif time_shift == 3 * 4:
                plt.plot(ts_hh["energy_net"],
                         label="Shifted +3 h",
                         linewidth=0.8,
                         color="navy",
                         linestyle="dashdot")
            elif time_shift == 6 * 4:
                plt.plot(ts_hh["energy_net"],
                         label="Shifted +6 h",
                         linewidth=0.8,
                         color="red",
                         linestyle="dashed")

            plt.ylim((-600, 0))
            plt.xlim((ts_hh.index[0], ts_hh.index[-1]))
            plt.ylabel("Power (W)")
            plt.legend()
            plt.grid(1, which="major", axis="both")


        plt.show()
        plt.close()
