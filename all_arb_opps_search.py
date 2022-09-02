from valr_api import ValrAPI
import itertools
from datetime import datetime, timedelta
from operator import itemgetter
import math
import pandas as pd
import os
import json


# global functions to put everything together
def calculate_routes(curr_base_currency, curr_pairs=None):
    arb_box = ArbContainer(curr_base_currency, curr_pairs)
    # create all the routes
    arb_box.get_routes()

    for i in arb_box.arb_routes:
        route = ArbRoute(arb_box.base_currency, i, arb_box.currs)
        route.theoretical_arb()
        route.set_pair_query_time()
        arb_box.add_object(route)

    return arb_box


def show_routes(arb_box):

    for i in arb_box.arb_objs:

        print(i.percentage_arb_opp, i.route_viz, i.rev_percentage_arb_opp)
        print('\t'+str(i.pairs_query_time))
        print('pair_dicts', i.pair_dicts)


def write_order_pair_json(arb_box):
    order_pair_list = []

    for i in arb_box.arb_objs:
        temp_dict = {}

        temp_dict['order'] = i.route_order
        temp_dict['pairs'] = [j['currencyPair'] for j in i.pair_dicts]

        order_pair_list.append(temp_dict)

        with open('data.json', 'w') as file:
            json.dump(order_pair_list, file)


class ArbContainer:

    def __init__(self, base_currency, curr_pairs=None):
        self.base_currency = base_currency
        self.arb_objs = []
        self.curr_pairs = curr_pairs

        self.arb_routes = []  # pair legs to create actual arb routes

        self.df_arb_box = None

        # all currency pairs availible
        if self.curr_pairs is None:
            self.currs = ValrAPI().get_currency_pairs()
        elif self.curr_pairs is not None:
            self.currs = []
            for curr in self.curr_pairs:
                pair_data = ValrAPI().market_summary_pair(curr)

                pair_data['baseCurrency'] = pair_data['currencyPair'][0:3]
                pair_data['quoteCurrency'] = pair_data['currencyPair'][3:]

                self.currs.append(pair_data)

    def get_routes(self):

        # all potential legs
        pot_legs = []

        # get all potential legs for target currency

        if self.curr_pairs is None:

            for curr in self.currs:

                if self.base_currency == curr['baseCurrency']:
                    pot_legs.append(curr['quoteCurrency'])

                elif self.base_currency == curr['quoteCurrency']:
                    pot_legs.append(curr['baseCurrency'])

        else:
            for curr in self.currs:
                pot_legs.extend([curr['baseCurrency'], curr['quoteCurrency']])

            pot_legs = list(set(pot_legs))
            pot_legs = [leg for leg in pot_legs if leg != self.base_currency]

        combinations = [set(combination) for combination in itertools.combinations(pot_legs, 2)]

        fruitful_combinations = []  # currency pairs
        for combination in combinations:
            for curr in self.currs:
                if combination == {curr['baseCurrency'], curr['quoteCurrency']}:
                    fruitful_combinations.append((curr['baseCurrency'], curr['quoteCurrency']))

        for f_combination in fruitful_combinations:
            get_listed = list(f_combination)
            self.arb_routes.append(
                tuple(tuple([(self.base_currency, get_listed[0]), tuple(get_listed), (get_listed[1], self.base_currency)])))

    def add_object(self, arb_obj):
        self.arb_objs.append(arb_obj)

    def write_df(self, file_name):
        route_dicts = []

        for i in self.arb_objs:
            route_dicts.append(
                {
                    'route_sequence': i.route_order,
                    'currency_pairs': [j['currencyPair'] for j in i.pair_dicts],
                    'ask': i.ask,
                    'bid': i.bid,
                    'spreads (%)': i.spread,
                    'base_min_order (R)': i.base_currency_price, ##
                    'data_time': [i.pairs_query_time[j]['db_time'] for j in i.pairs_query_time.keys()], ##
                    'delta_time': [i.pairs_query_time[j]['delta_time'] for j in i.pairs_query_time.keys()],
                    'forward_yield': i.percentage_arb_opp,
                    'backward_yield': i.rev_percentage_arb_opp
                }
            )

        self.df_arb_box = pd.DataFrame(route_dicts)

        if not os.path.isfile(f'./Data/{file_name}'):
            self.df_arb_box.to_csv(f'./Data/{file_name}', mode='w', index=False)
        else:
            self.df_arb_box.to_csv(f'./Data/{file_name}', mode='a', index=False, header=False)




class ArbRoute:
    def __init__(self, base_currency, arb_route, currs):
        self.base_currency = base_currency

        self.route = arb_route
        self.currs = currs

        self.pair_dicts = []
        self.pair_prices_dicts = []

        self.percentage_arb_opp = None
        self.rev_percentage_arb_opp = None

        self.pairs_query_time = None

        self.spread = []
        self.ask = []
        self.bid = []

        self.base_currency_price = []

        market_inst = ValrAPI()
        price_pair_dicts = market_inst.market_summary()


        for pair in self.route:
            for curr in self.currs:
                if set(pair) == {curr['baseCurrency'], curr['quoteCurrency']}:
                    self.pair_dicts.append(curr)


                    for price_pair in price_pair_dicts:
                        if price_pair['currencyPair'] == curr['baseCurrency'] + curr['quoteCurrency']:
                            self.pair_prices_dicts.append(price_pair)

        self.route_order = (arb_route[0][0], arb_route[0][1], arb_route[2][0], arb_route[2][1])

        self.route_viz = f'{self.route_order[0]} <-> {self.route_order[1]} <-> {self.route_order[2]} <-> {self.route_order[3]}'

    def theoretical_arb(self):

        current_amt = 1

        try:
            for i in range(len(self.route)):

                if self.route[i][0] == self.pair_dicts[i]['baseCurrency']:
                    current_amt = current_amt * float(self.pair_prices_dicts[i]['bidPrice'])  # bidPrice, should times
                else:
                    current_amt = current_amt / float(self.pair_prices_dicts[i]['askPrice'])  # askPrice, should div

                self.percentage_arb_opp = (current_amt / 1) * 100

        except IndexError:

            self.percentage_arb_opp = 'Pair not found'

        current_amt = 1

        try:

            rev_pair_dicts = self.pair_dicts[::-1]
            rev_pair_prices_dict = self.pair_prices_dicts[::-1]
            rev_route = tuple([i[::-1] for i in self.route][::-1])

            for i in range(len(rev_route)):

                if rev_route[i][0] == rev_pair_dicts[i]['baseCurrency']:
                    current_amt = current_amt * float(rev_pair_prices_dict[i]['bidPrice'])  # bidPrice, should times
                else:
                    current_amt = current_amt / float(rev_pair_prices_dict[i]['askPrice'])  # askPrice, should div

                self.rev_percentage_arb_opp = (current_amt / 1) * 100

        except IndexError:

            self.rev_percentage_arb_opp = 'Pair not found'

    def set_pair_query_time(self):

        def _format_time(j):
            print(j)
            return datetime.strptime(j['created'], '%Y-%m-%dT%H:%M:%S.%fZ')

        def _get_delta(j):

            server_time = _format_time(j)

            return timedelta(minutes=datetime.now().minute, seconds=datetime.now().second).seconds - timedelta(minutes=server_time.minute, seconds=server_time.second).seconds

        # create time stamp for prices used and deltatime for each pair from time queried.
        self.pairs_query_time = dict([(j['currencyPair'], dict([('db_time', _format_time(j)), ('delta_time', _get_delta(j))])) for j in self.pair_prices_dicts])

    def min_base_value(self, curr_pair):

        def get_min_order(keys):
            val_list = []

            for i in keys:
                quantity_price = [float(j) for j in itemgetter('quantity', 'price')(ob[i][0])]
                val_list.append(quantity_price)

            spread = val_list[0][1]-val_list[1][1]

            val_list_prod = [math.prod(i) for i in val_list]

            return min(val_list_prod), spread, val_list[0][1], val_list[1][1]

        ob = ValrAPI().get_order_book(curr_pair)

        dict_keys = ['Asks', 'Bids']

        # get min order I can place
        min_order_value, single_spread, ask, bid = get_min_order(dict_keys)

        self.ask.append(ask)
        self.bid.append(bid)

        self.spread.append(single_spread)

        # if 'quoteCurrency' for pair == 'ZAR' -> do nothing
        if curr_pair[3:] == 'ZAR':
            base_value = min_order_value
        else:
            base_value = min_order_value * (float([j['lastTradedPrice'] for j in self.pair_dicts if j['currencyPair'] == curr_pair[3:]+'ZAR'][0]))
        print('base_value', base_value)
        self.base_currency_price.append(base_value)

    def set_base_currency_price(self):
        self.base_currency_price = min(self.base_currency_price)


if __name__ == '__main__':

    base_currency = 'ETH'

    arb_box = calculate_routes(base_currency)

    show_routes(arb_box)
