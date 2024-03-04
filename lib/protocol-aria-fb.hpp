
namespace spectrum
{

class Aria {

    private:
    void Join(std::function<void(&AriaTransaction)>, std::vector<AriaTransaction>);

    public:
    Aria(/* args */);
    ~Aria();

};

class AriaTransaction {

};

class AriaExecutor {

};

} // namespace spectrum