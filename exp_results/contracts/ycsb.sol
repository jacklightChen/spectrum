pragma solidity >=0.7.0 <0.9.0;

contract YCSB_10key_10r5w {

    mapping(uint256=>uint256) store;

    function set(uint256 key1, uint256 key2, uint256 key3, uint256 key4, uint256 key5, uint256 key6, uint256 key7, uint256 key8, uint256 key9, uint256 key10, uint256 value) public {
        store[key1] = store[key1] + value;
        uint256 load2 = store[key2];
        store[key3] = store[key3] + value;
        uint256 load4 = store[key4];
        store[key5] = store[key5] + value;
        uint256 load6 = store[key6];
        store[key7] = store[key7] + value;
        uint256 load8 = store[key8];
        store[key9] = store[key9] + value;
        uint256 load10 = store[key10];
    }

    function get(uint256 key) public view returns (uint256){
        return store[key];
    }

    function init(uint256 key, uint256 value) public {
        store[key] = value;
    }
}
