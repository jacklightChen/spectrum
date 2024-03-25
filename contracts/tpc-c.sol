// SPDX-License-Identifier: GPL-3.0

pragma solidity >=0.8.2 <0.9.0;

contract TPCCStorage {

    struct Warehouse {
        uint256 w_id;
    }

    struct District {
        uint256 d_id;
        uint256 d_next_o_id;
    }

    struct Customer {
        uint256 w_id;
        uint256 d_id;
        uint256 c_id;
    }

    struct NewOrder {
        uint256 no_w_id;
        uint256 no_d_id;
        uint256 no_o_id;
    }

    struct Order {
        uint256 o_id;
        uint256 o_d_id;
        uint256 o_w_id;
        uint256 o_c_id;
        uint256 o_entry_d;
        uint256 o_carrier_id;
        uint256 o_ol_cnt;
        bool o_all_local;
    }

    struct Item {
        uint256 i_id;
        uint256 i_price;
        uint256 s_quantity;
        uint256 brand_generic;
    }

    struct Stock {
        uint256 i_w_id;
        uint256 i_id;
        uint256 s_quantity;
        uint256 s_order_cnt;
        uint256 s_ytd;
        uint256 s_remote_cnt;
        uint256[] dist_info;
    }

    struct Orderline {
        uint256 ol_o_id;
        uint256 ol_d_id;
        uint256 ol_w_id;
        uint256 ol_number;
        uint256 ol_supply_w_id;
        uint256 ol_i_id;
        uint256 ol_quantity;
        uint256 ol_amount;
        uint256 ol_dist_info;
    }

    mapping (uint256 => Warehouse) warehouse_map;
    mapping (uint256 => District)  district_map;
    mapping (uint256 => Customer)  customer_map;
    mapping (uint256 => NewOrder)  neworder_map;
    mapping (uint256 => mapping(uint256 => mapping(uint256 => Order)))     order_map;
    mapping (uint256 => Item)      item_map;
    mapping (uint256 => mapping(uint256 => Stock))     stock_map;
    mapping (uint256 => mapping(uint256 => mapping (uint256 => mapping (uint256 => Orderline)))) orderline_map;

    function new_order(uint256 w_id, uint256 d_id, uint256 c_id, uint256 o_entry_d, uint256[] calldata i_ids, uint256[] calldata i_w_ids, uint256[] calldata i_qtys) public {
        District    storage district    = district_map[d_id];
        district.d_next_o_id            += 1;
        neworder_map[district.d_next_o_id] = NewOrder({
            no_o_id: district.d_next_o_id,
            no_d_id: district.d_id,
            no_w_id: w_id
        });
        bool all_items_local = true;
        uint256 o_id = district.d_next_o_id;
        for (uint256 i = 0; i < i_w_ids.length; i += 1) {
            if (i_w_ids[i] != w_id) all_items_local = false;
        }
        order_map[w_id][d_id][district.d_next_o_id] = Order({
            o_id: district.d_next_o_id,
            o_d_id: d_id,
            o_w_id: w_id,
            o_c_id: c_id,
            o_entry_d: o_entry_d,
            o_carrier_id: 0,
            o_ol_cnt: i_ids.length,
            o_all_local: all_items_local
        });
        uint256 total_ol_amount = 0;
        uint256 ol_d_id = d_id;
        for (uint256 i = 0; i < i_ids.length; i += 1) {
            if (stock_map[i_ids[i]][i_w_ids[i]].s_quantity >= i_qtys[i] + 10) {
                stock_map[i_ids[i]][i_w_ids[i]].s_quantity -= i_qtys[i];
            }
            else {
                stock_map[i_ids[i]][i_w_ids[i]].s_quantity += 91;
                stock_map[i_ids[i]][i_w_ids[i]].s_quantity -= i_qtys[i];
            }
            stock_map[i_ids[i]][i_w_ids[i]].s_order_cnt += 1;
            stock_map[i_ids[i]][i_w_ids[i]].s_ytd += i_qtys[i];
            if (i_w_ids[i] != w_id) {
                stock_map[i_ids[i]][i_w_ids[i]].s_remote_cnt += 1;
            }
            uint256 ol_amount = i_qtys[i] * item_map[i_ids[i]].i_price;
            total_ol_amount += ol_amount;
            // write this dummy shit because of 'call stack too deep'
            uint256 ol_i_id = i_ids[i];
            uint256 ol_w_id = i_w_ids[i];
            uint256 ol_number = i + 1;
            orderline_map[ol_w_id][ol_d_id][o_id][ol_number] = Orderline({
                ol_o_id: o_id,
                ol_d_id: ol_d_id,
                ol_w_id: ol_w_id,
                ol_number: ol_number,
                ol_supply_w_id: i_w_ids[i],
                ol_i_id: ol_i_id,
                ol_quantity: i_qtys[i],
                ol_amount : ol_amount,
                ol_dist_info: stock_map[ol_i_id][ol_w_id].dist_info[ol_d_id]
            });
        }
        return;
    }

}
