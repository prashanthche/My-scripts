import json
import clickhouse_connect
import logging

# Set up logging with debug level for detailed output
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Define connection details
host_name = "10.53.97.71"
port_nr = 8123
username = ""  # No username
creds = ""  # No password

# Define the SQL query
query_instructions = """
    SELECT table_round_id, player_round_data 
    FROM lc_warehouse.cards_cancelled_player_round_data 
    WHERE table_round_id = '{table_round_id}'
"""

# Card values mapping
CARD_VALUES = {
    '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, '10': 10,
    'J': 10, 'Q': 10, 'K': 10, 'A': 11
}

def parse_card_value(card):
    """Extracts and returns the numeric value of a card."""
    if not card:
        logging.debug("Empty card string")
        return 0

    # Extract the value part (assuming suit is the first character)
    card_value = card[1:] if len(card) > 1 else card
    numeric_value = CARD_VALUES.get(card_value, 0)
    
    if numeric_value == 0:
        logging.warning(f"Unrecognized card value: {card_value} from card: {card}")
    
    logging.debug(f"Card: {card}, Parsed Value: {numeric_value}")
    return numeric_value

class CasinoCalculator:
    def __init__(self):
        pass

    def calculate_hand_value(self, cards):
        """Calculates the total value of a hand of cards."""
        value = 0
        aces = 0
        for card in cards:
            card_value = parse_card_value(card)
            value += card_value
            if card_value == 11:  # Ace
                aces += 1
        while value > 21 and aces:
            value -= 10
            aces -= 1
        logging.debug(f"Hand Cards: {cards}, Calculated Value: {value}")
        return value

    def calculate_side_bets(self, player_data):
        """Calculates side bet payouts."""
        side_bet_payouts = {}
        # Updated payout ratios for different side bets
        side_bet_payout_ratios = {
            "Mixed Color Pair": 6,
            "Same Color Pair": 12,
            "Golden Pair": 25,
            "Flush": 5,
            "Straight": 10,
            "Three of a Kind": 30,
            "Straight Flush": 40,
            "Suited Trips": 100,
            "Three of a Kind Suited": 270,
            # Removed duplicate "Straight Flush" and "Three of a Kind" keys
        }
        
        for seat_id, data in player_data.items():
            side_bets = [bet for bet in data.get("betsResults", []) if bet.get("betType") != "INITIAL_BET"]
            total_side_bet_payout = 0
            
            for bet in side_bets:
                bet_type = bet.get("betType")
                bet_amount = bet.get("bet", 0)
                payout_ratio = side_bet_payout_ratios.get(bet_type, 0)
                total_side_bet_payout += bet_amount * payout_ratio
            
            side_bet_payouts[seat_id] = total_side_bet_payout
        return side_bet_payouts

    def calculate_payout(self, dealer_cards, player_data):
        """Calculates the total payout based on the dealer's and player's hands."""
        total_payout = 0
        results = []

        dealer_value = self.calculate_hand_value(dealer_cards)
        side_bet_payouts = self.calculate_side_bets(player_data)

        for seat_id, data in player_data.items():
            player_id = data.get("player_id", f"Player-{seat_id}")
            player_cards = data["handsResults"][0]["cardValues"] if data.get("handsResults") else []
            initial_bet = data["betsResults"][0]["bet"] if data.get("betsResults") and len(data["betsResults"]) > 0 else 0
            side_bet_payout = side_bet_payouts.get(seat_id, 0)

            player_value = self.calculate_hand_value(player_cards)
            logging.debug(f"Player ID: {player_id}, Player Cards: {player_cards}, Player Value: {player_value}, Dealer Value: {dealer_value}, Initial Bet: {initial_bet}")

            result = {
                "player_id": player_id,
                "seat_id": seat_id,
                "initial_bet": initial_bet,
                "payout": 0,
                "details": f"Hand 1: Cards - {player_cards}, Value - {player_value}, Dealer Cards - {dealer_cards}",
                "side_bets": {bet["betType"]: bet["bet"] for bet in data.get("betsResults", []) if bet.get("betType") != "INITIAL_BET"}
            }

            if player_value == 21 and len(player_cards) == 2:
                if dealer_value == 21 and len(dealer_cards) == 2:
                    result["payout"] = initial_bet  # Push
                    result["details"] += ", Push"
                else:
                    result["payout"] = initial_bet * 1.5  # Black Jack
                    result["details"] += ", Black Jack"
            elif player_value == dealer_value:
                result["payout"] = initial_bet  # Push
                result["details"] += ", Push"
            elif player_value > dealer_value or dealer_value > 21:
                result["payout"] = initial_bet * 2  # Hand Win
                result["details"] += ", Hand Win"
            else:
                result["payout"] = 0
                result["details"] += ", Loss"

            result["payout"] += side_bet_payout

            total_payout += result["payout"]
            results.append(result)

        return total_payout, results

def connect_to_database(host, port, username, password):
    """Connects to the database and returns the client."""
    try:
        client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
        return client
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None

def fetch_player_round_data(client, table_round_id):
    """Fetches player round data from the database."""
    try:
        query = query_instructions.format(table_round_id=table_round_id)
        response = client.query(query)
        return response.result_rows
    except Exception as e:
        logging.error(f"Query failed: {e}")
        return None

def main():
    """Main function to execute the script."""
    # Connect to the database
    connection = connect_to_database(host_name, port_nr, username, creds)
    if connection:
        # Fetch data from the database
        table_round_id = input("Enter Table Round ID: ")
        data = fetch_player_round_data(connection, table_round_id)

        if data:
            # Initialize the casino calculator
            calculator = CasinoCalculator()

            # Process each row of data
            logging.info(f"\nProcessing data for Table Round ID: {table_round_id}")
            for row in data:
                table_round_id, player_round_data = row

                # Parse the player round data
                try:
                    player_round_data_dict = json.loads(player_round_data)
                except Exception as e:
                    logging.error(f"Failed to parse player_round_data: {e}")
                    continue

                dealer_cards = player_round_data_dict.get("dealerCards", [])
                player_data = player_round_data_dict.get("playerRoundHistBj", {})

                # Calculate the payout
                total_payout, results = calculator.calculate_payout(
                    dealer_cards,
                    player_data
                )

                logging.info(f"Total Payout: €{total_payout}")
                for result in results:
                    logging.info(f"\nPlayer ID: {result['player_id']}")
                    logging.info(f"Seat ID: {result['seat_id']}")
                    logging.info(f"Initial Bet: €{result['initial_bet']}")
                    logging.info(f"Payout: €{result['payout']}")
                    logging.info(f"Details: {result['details']}")
                    logging.info(f"Side Bets: {result['side_bets']}\n")
        else:
            logging.info("No data found for the given Table Round ID.")
    else:
        logging.info("Failed to connect to the database.")

if __name__ == "__main__":
    main()