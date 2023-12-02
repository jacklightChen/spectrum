pragma solidity >=0.7.0 <0.9.0;

contract Smallbank {
    mapping(uint256=>uint256) savingStore;
    mapping(uint256=>uint256) checkingStore;
    mapping(uint256=>uint256) accountStore; 
    // In the original version of the benchmark, this is suppose to be a look up on the customer's name.
    // we preserve it to simulate customer's info

    function getBalance(uint256 addr) public returns (uint256 balance) {
        uint256 info0 = accountStore[addr];

        uint256 bal0 = savingStore[addr];
        uint256 bal1 = checkingStore[addr];

        uint256 balance = bal0 + bal1;
        return balance;
    }

    function depositChecking(uint256 addr, uint256 bal) public {
        uint256 info0 = accountStore[addr];

        uint256 bal0 = checkingStore[addr];
        uint256 bal1 = bal;
        
        
        checkingStore[addr] = bal0 + bal1;
    }

    function writeCheck(uint256 addr, uint256 bal) public {
        uint256 info0 = accountStore[addr];

        uint256 bal1 = checkingStore[addr];

        uint256 amount = bal;
      
        if (amount <= bal1) {
            checkingStore[addr] = bal1 - amount;
        } else {
            checkingStore[addr] = 0;
        }
    }
    
    function transactSaving(uint256 addr, uint256 bal) public {
        uint256 info0 = accountStore[addr];

        uint256 bal0 = savingStore[addr];
        uint256 bal1 = bal;
        
        savingStore[addr] = bal0 + bal1;
    }

    function sendPayment(uint256 addr0, uint256 addr1, uint256 bal) public {
        uint256 info0 = accountStore[addr0];
        uint256 info1 = accountStore[addr1];

        uint256 bal0 = checkingStore[addr0];
        uint256 bal1 = checkingStore[addr1];
        uint256 amount = bal;

        if (bal0 >= amount) {
            bal0 -= amount;
            bal1 += amount;
            
            checkingStore[addr0] = bal0;
            checkingStore[addr1] = bal1;
        }
    }

    function amalgamate(uint256 addr0, uint256 addr1) public {
       uint256 info1 = accountStore[addr0];
       uint256 info2 = accountStore[addr1];

       uint bal0 = savingStore[addr0];
       uint bal1 = checkingStore[addr1];
       
       checkingStore[addr1] = 0;
       savingStore[addr0] = bal0 + bal1;
    } 

    function init(uint256 key, uint256 value) public {
        savingStore[key] = value;
        checkingStore[key] = value;
        accountStore[key] = value;
    }
}
