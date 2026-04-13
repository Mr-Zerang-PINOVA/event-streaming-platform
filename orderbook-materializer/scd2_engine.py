 # reuse your scd2_over_events logic (adapted)

def apply_event(state, event):
    outputs = []

    t = event["event_time"]

    for side_name in ["bids", "asks"]:
        book = state.bids if side_name == "bids" else state.asks

        for price, qty in event[side_name]:

            # DELETE
            if qty == 0:
                if price in book:
                    prev = book.pop(price)
                    outputs.append({
                        "side": side_name[:-1],
                        "price": price,
                        "quantity": prev["qty"],
                        "valid_from": prev["valid_from"],
                        "valid_to": t
                    })
                continue

            # UPDATE
            if price in book:
                prev = book[price]
                if prev["qty"] != qty:
                    outputs.append({
                        "side": side_name[:-1],
                        "price": price,
                        "quantity": prev["qty"],
                        "valid_from": prev["valid_from"],
                        "valid_to": t
                    })

            # OPEN NEW
            book[price] = {
                "qty": qty,
                "valid_from": t
            }

    return outputs
