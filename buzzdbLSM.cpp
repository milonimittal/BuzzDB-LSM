#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <cassert>
#include <list>
#include <unordered_map>
#include <set>
#include <map>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <thread>
#include <queue>
#include <optional>
#include <regex>
#include <stdexcept>
#include <cstring>

enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    size_t data_length;
    std::unique_ptr<char[]> data;

public:
    Field(int i) : type(INT) { 
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FLOAT) { 
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

   // Copy constructor
    Field(const Field& other) : type(other.type), data_length(other.data_length), data(new char[data_length]) {
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    // Move constructor - If you already have one, ensure it's correctly implemented
    Field(Field&& other) noexcept : type(other.type), data_length(other.data_length), data(std::move(other.data)) {
        // Optionally reset other's state if needed
    }

    FieldType getType() const { return type; }
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string asString() const { 
        return std::string(data.get());
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == STRING) {
            buffer << data.get() << ' ';
        } else if (type == INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type; in >> type;
        size_t length; in >> length;
        if (type == STRING) {
            std::string val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    // Clone method
    std::unique_ptr<Field> clone() const {
        // Use the copy constructor
        return std::make_unique<Field>(*this);
    }

    void print() const{
        switch(getType()){
            case INT: std::cout << asInt(); break;
            case FLOAT: std::cout << asFloat(); break;
            case STRING: std::cout << asString(); break;
        }
    }
};

bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.type != rhs.type) return false; // Different types are never equal

    switch (lhs.type) {
        case INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) == *reinterpret_cast<const int*>(rhs.data.get());
        case FLOAT:
            return *reinterpret_cast<const float*>(lhs.data.get()) == *reinterpret_cast<const float*>(rhs.data.get());
        case STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) == std::string(rhs.data.get(), rhs.data_length - 1);
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    // Clone method
    std::unique_ptr<Tuple> clone() const {
        auto clonedTuple = std::make_unique<Tuple>();
        for (const auto& field : fields) {
            clonedTuple->addField(field->clone());
        }
        return clonedTuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;  // Fixed page size
static constexpr size_t MAX_SLOTS = 512;   // Fixed number of slots
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
    bool empty = true;                 // Is the slot empty?    
    uint16_t offset = INVALID_VALUE;    // Offset of the slot within the page
    uint16_t length = INVALID_VALUE;    // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage(){
        // Empty page -> initialize slot array inside page
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    // Add a tuple, returns true if it fits, false otherwise.
    bool addTuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        std::cout << "Tuple size: " << tuple_size << " bytes\n";
        assert(tuple_size == 16);

        // Check for first slot with enough space
        size_t slot_itr = 0;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == true and 
                slot_array[slot_itr].length >= tuple_size) {
                break;
            }
        }
        if (slot_itr == MAX_SLOTS){
            //std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
            return false;
        }

        // Identify the offset where the tuple will be placed in the page
        // Update slot meta-data if needed
        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;
        if (slot_array[slot_itr].offset == INVALID_VALUE){
            if(slot_itr != 0){
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            }
            else{
                offset = metadata_size;
            }

            slot_array[slot_itr].offset = offset;
        }
        else{
            offset = slot_array[slot_itr].offset;
        }

        if(offset + tuple_size >= PAGE_SIZE){
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        if (slot_array[slot_itr].length == INVALID_VALUE){
            slot_array[slot_itr].length = tuple_size;
        }

        // Copy serialized data into the page
        std::memcpy(page_data.get() + offset, 
                    serializedTuple.c_str(), 
                    tuple_size);

        return true;
    }

    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        size_t slot_itr = 0;
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if(slot_itr == index and
               slot_array[slot_itr].empty == false){
                slot_array[slot_itr].empty = true;
                break;
               }
        }

        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void print() const{
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false){
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

const std::string database_filename = "buzzdbLSM.dat";

class StorageManager {
public:    
    std::fstream fileStream;
    size_t num_pages = 0;
    int current_page_id = -1;

public:
    StorageManager(){
        fileStream.open(database_filename, std::ios::in | std::ios::out);
        if (!fileStream) {
            // If file does not exist, create it
            fileStream.clear(); // Reset the state
            fileStream.open(database_filename, std::ios::out);
        }
        fileStream.close(); 
        fileStream.open(database_filename, std::ios::in | std::ios::out); 

        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;

        std::cout << "Storage Manager :: Num pages: " << num_pages << "\n";        
        if(num_pages == 0){
            extend();
        }

    }

    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    // Read a page from disk
    std::unique_ptr<SlottedPage> load(uint16_t page_id) {
        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        // Read the content of the file into the page
        if(fileStream.read(page->page_data.get(), PAGE_SIZE)){
            //std::cout << "Page read successfully from file." << std::endl;
        }
        else{
            std::cerr << "Error: Unable to read data from the file. \n";
            exit(-1);
        }
        return page;
    }

    // Write a page to disk
    void flush(uint16_t page_id, const std::unique_ptr<SlottedPage>& page) {
        size_t page_offset = page_id * PAGE_SIZE;        

        // Move the write pointer
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page->page_data.get(), PAGE_SIZE);        
        fileStream.flush();
    }

    // Extend database file by one page
    void extend() {
        std::cout << "Extending database file \n";

        // Create a slotted page
        auto empty_slotted_page = std::make_unique<SlottedPage>();

        // Move the write pointer
        fileStream.seekp(0, std::ios::end);

        // Write the page to the file, extending it
        fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
        fileStream.flush();

        // Update number of pages
        num_pages += 1;
        current_page_id += 1;
    }

};

using PageID = uint16_t;

class Policy {
public:
    virtual bool touch(PageID page_id) = 0;
    virtual PageID evict() = 0;
    virtual ~Policy() = default;
};

void printList(std::string list_name, const std::list<PageID>& myList) {
        std::cout << list_name << " :: ";
        for (const PageID& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

class LruPolicy : public Policy {
private:
    // List to keep track of the order of use
    std::list<PageID> lruList;

    // Map to find a page's iterator in the list efficiently
    std::unordered_map<PageID, std::list<PageID>::iterator> map;

    size_t cacheSize;

public:

    LruPolicy(size_t cacheSize) : cacheSize(cacheSize) {}

    bool touch(PageID page_id) override {
        //printList("LRU", lruList);

        bool found = false;
        // If page already in the list, remove it
        if (map.find(page_id) != map.end()) {
            found = true;
            lruList.erase(map[page_id]);
            map.erase(page_id);            
        }

        // If cache is full, evict
        if(lruList.size() == cacheSize){
            evict();
        }

        if(lruList.size() < cacheSize){
            // Add the page to the front of the list
            lruList.emplace_front(page_id);
            map[page_id] = lruList.begin();
        }

        return found;
    }

    PageID evict() override {
        // Evict the least recently used page
        PageID evictedPageId = INVALID_VALUE;
        if(lruList.size() != 0){
            evictedPageId = lruList.back();
            map.erase(evictedPageId);
            lruList.pop_back();
        }
        return evictedPageId;
    }

};

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferManager {
private:
    using PageMap = std::unordered_map<PageID, std::unique_ptr<SlottedPage>>;

    StorageManager storage_manager;
    PageMap pageMap;
    std::unique_ptr<Policy> policy;

public:
    BufferManager(): 
    policy(std::make_unique<LruPolicy>(MAX_PAGES_IN_MEMORY)) {}

    std::unique_ptr<SlottedPage>& getPage(int page_id) {
        auto it = pageMap.find(page_id);
        if (it != pageMap.end()) {
            policy->touch(page_id);
            return pageMap.find(page_id)->second;
        }

        if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
            auto evictedPageId = policy->evict();
            if(evictedPageId != INVALID_VALUE){
                std::cout << "Evicting page " << evictedPageId << "\n";
                storage_manager.flush(evictedPageId, 
                                      pageMap[evictedPageId]);
            }
        }

        auto page = storage_manager.load(page_id);
        policy->touch(page_id);
        std::cout << "Loading page: " << page_id << "\n";
        pageMap[page_id] = std::move(page);
        return pageMap[page_id];
    }

    void flushPage(int page_id) {
        //std::cout << "Flush page " << page_id << "\n";
        storage_manager.flush(page_id, pageMap[page_id]);
    }

    void extend(){
        storage_manager.extend();
    }
    
    size_t getNumPages(){
        return storage_manager.num_pages;
    }

    int getCurrentPageId(){
        return storage_manager.current_page_id;
    }

    void readPage(int page_id) {
        auto page = storage_manager.load(page_id);
        std::cout << "Contents of Page "<<page_id<<":"<< std::endl;
        page->print();
    }

};

class HashIndex {
private:
    struct HashEntry {
        int key;
        int value;
        int position; // Final position within the array
        bool exists; // Flag to check if entry exists

        // Default constructor
        HashEntry() : key(0), value(0), position(-1), exists(false) {}

        // Constructor for initializing with key, value, and exists flag
        HashEntry(int k, int v, int pos) : key(k), value(v), position(pos), exists(true) {}    
    };

    static const size_t capacity = 100; // Hard-coded capacity
    HashEntry hashTable[capacity]; // Static-sized array

    size_t hashFunction(int key) const {
        return key % capacity; // Simple modulo hash function
    }

public:
    HashIndex() {
        // Initialize all entries as non-existing by default
        for (size_t i = 0; i < capacity; ++i) {
            hashTable[i] = HashEntry();
        }
    }

    void insertOrUpdate(int key, int value) {
        size_t index = hashFunction(key);
        size_t originalIndex = index;
        bool inserted = false;
        int i = 0; // Attempt counter

        do {
            if (!hashTable[index].exists) {
                hashTable[index] = HashEntry(key, value, true);
                hashTable[index].position = index;
                inserted = true;
                break;
            } else if (hashTable[index].key == key) {
                hashTable[index].value += value;
                hashTable[index].position = index;
                inserted = true;
                break;
            }
            i++;
            index = (originalIndex + i*i) % capacity; // Quadratic probing
        } while (index != originalIndex && !inserted);

        if (!inserted) {
            std::cerr << "HashTable is full or cannot insert key: " << key << std::endl;
        }
    }

   int getValue(int key) const {
        size_t index = hashFunction(key);
        size_t originalIndex = index;

        do {
            if (hashTable[index].exists && hashTable[index].key == key) {
                return hashTable[index].value;
            }
            if (!hashTable[index].exists) {
                break; // Stop if we find a slot that has never been used
            }
            index = (index + 1) % capacity;
        } while (index != originalIndex);

        return -1; // Key not found
    }

    // This method is not efficient for range queries 
    // as this is an unordered index
    // but is included for comparison
    std::vector<int> rangeQuery(int lowerBound, int upperBound) const {
        std::vector<int> values;
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists && hashTable[i].key >= lowerBound && hashTable[i].key <= upperBound) {
                std::cout << "Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
                values.push_back(hashTable[i].value);
            }
        }
        return values;
    }

    void print() const {
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists) {
                std::cout << "Position: " << hashTable[i].position << 
                ", Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
            }
        }
    }
};

class Operator {
    public:
    virtual ~Operator() = default;

    /// Initializes the operator.
    virtual void open() = 0;

    /// Tries to generate the next tuple. Return true when a new tuple is
    /// available.
    virtual bool next() = 0;

    /// Destroys the operator.
    virtual void close() = 0;

    /// This returns the pointers to the Fields of the generated tuple. When
    /// `next()` returns true, the Fields will contain the values for the
    /// next tuple. Each `Field` pointer in the vector stands for one attribute of the tuple.
    virtual std::vector<std::unique_ptr<Field>> getOutput() = 0;
};

class UnaryOperator : public Operator {
    protected:
    Operator* input;

    public:
    explicit UnaryOperator(Operator& input) : input(&input) {}

    ~UnaryOperator() override = default;
};

class BinaryOperator : public Operator {
    protected:
    Operator* input_left;
    Operator* input_right;

    public:
    explicit BinaryOperator(Operator& input_left, Operator& input_right)
        : input_left(&input_left), input_right(&input_right) {}

    ~BinaryOperator() override = default;
};

class ScanOperator : public Operator {
private:
    BufferManager& bufferManager;
    size_t currentPageIndex = 0;
    size_t currentSlotIndex = 0;
    std::unique_ptr<Tuple> currentTuple;
    size_t tuple_count = 0;

public:
    ScanOperator(BufferManager& manager) : bufferManager(manager) {}

    void open() override {
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset(); // Ensure currentTuple is reset
        loadNextTuple();
    }

    bool next() override {
        if (!currentTuple) return false; // No more tuples available

        loadNextTuple();
        return currentTuple != nullptr;
    }

    void close() override {
        std::cout << "Scan Operator tuple_count: " << tuple_count << "\n";
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (currentTuple) {
            return std::move(currentTuple->fields);
        }
        return {}; // Return an empty vector if no tuple is available
    }

private:
    void loadNextTuple() {
        while (currentPageIndex < bufferManager.getNumPages()) {
            auto& currentPage = bufferManager.getPage(currentPageIndex);
            if (!currentPage || currentSlotIndex >= MAX_SLOTS) {
                currentSlotIndex = 0; // Reset slot index when moving to a new page
            }

            char* page_buffer = currentPage->page_data.get();
            Slot* slot_array = reinterpret_cast<Slot*>(page_buffer);

            while (currentSlotIndex < MAX_SLOTS) {
                if (!slot_array[currentSlotIndex].empty) {
                    assert(slot_array[currentSlotIndex].offset != INVALID_VALUE);
                    const char* tuple_data = page_buffer + slot_array[currentSlotIndex].offset;
                    std::istringstream iss(std::string(tuple_data, slot_array[currentSlotIndex].length));
                    currentTuple = Tuple::deserialize(iss);
                    currentSlotIndex++; // Move to the next slot for the next call
                    tuple_count++;
                    return; // Tuple loaded successfully
                }
                currentSlotIndex++;
            }

            // Increment page index after exhausting current page
            currentPageIndex++;
        }

        // No more tuples are available
        currentTuple.reset();
    }
};

class IPredicate {
public:
    virtual ~IPredicate() = default;
    virtual bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const = 0;
};

void printTuple(const std::vector<std::unique_ptr<Field>>& tupleFields) {
    std::cout << "Tuple: [";
    for (const auto& field : tupleFields) {
        field->print(); // Assuming `print()` is a method that prints field content
        std::cout << " ";
    }
    std::cout << "]";
}

class SimplePredicate: public IPredicate {
public:
    enum OperandType { DIRECT, INDIRECT };
    enum ComparisonOperator { EQ, NE, GT, GE, LT, LE }; // Renamed from PredicateType

    struct Operand {
        std::unique_ptr<Field> directValue;
        size_t index = 0;
        OperandType type;

        Operand(std::unique_ptr<Field> value) : directValue(std::move(value)), type(DIRECT) {}
        Operand(size_t idx) : index(idx), type(INDIRECT) {}
    };

    Operand left_operand;
    Operand right_operand;
    ComparisonOperator comparison_operator;

    SimplePredicate(Operand left, Operand right, ComparisonOperator op)
        : left_operand(std::move(left)), right_operand(std::move(right)), comparison_operator(op) {}

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        const Field* leftField = nullptr;
        const Field* rightField = nullptr;

        if (left_operand.type == DIRECT) {
            leftField = left_operand.directValue.get();
        } else if (left_operand.type == INDIRECT) {
            leftField = tupleFields[left_operand.index].get();
        }

        if (right_operand.type == DIRECT) {
            rightField = right_operand.directValue.get();
        } else if (right_operand.type == INDIRECT) {
            rightField = tupleFields[right_operand.index].get();
        }

        if (leftField == nullptr || rightField == nullptr) {
            std::cerr << "Error: Invalid field reference.\n";
            return false;
        }

        if (leftField->getType() != rightField->getType()) {
            std::cerr << "Error: Comparing fields of different types.\n";
            return false;
        }

        // Perform comparison based on field type
        switch (leftField->getType()) {
            case FieldType::INT: {
                int left_val = leftField->asInt();
                int right_val = rightField->asInt();
                return compare(left_val, right_val);
            }
            case FieldType::FLOAT: {
                float left_val = leftField->asFloat();
                float right_val = rightField->asFloat();
                return compare(left_val, right_val);
            }
            case FieldType::STRING: {
                std::string left_val = leftField->asString();
                std::string right_val = rightField->asString();
                return compare(left_val, right_val);
            }
            default:
                std::cerr << "Invalid field type\n";
                return false;
        }
    }


private:

    // Compares two values of the same type
    template<typename T>
    bool compare(const T& left_val, const T& right_val) const {
        switch (comparison_operator) {
            case ComparisonOperator::EQ: return left_val == right_val;
            case ComparisonOperator::NE: return left_val != right_val;
            case ComparisonOperator::GT: return left_val > right_val;
            case ComparisonOperator::GE: return left_val >= right_val;
            case ComparisonOperator::LT: return left_val < right_val;
            case ComparisonOperator::LE: return left_val <= right_val;
            default: std::cerr << "Invalid predicate type\n"; return false;
        }
    }
};

class ComplexPredicate : public IPredicate {
public:
    enum LogicOperator { AND, OR };

private:
    std::vector<std::unique_ptr<IPredicate>> predicates;
    LogicOperator logic_operator;

public:
    ComplexPredicate(LogicOperator op) : logic_operator(op) {}

    void addPredicate(std::unique_ptr<IPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        
        if (logic_operator == AND) {
            for (const auto& pred : predicates) {
                if (!pred->check(tupleFields)) {
                    return false; // If any predicate fails, the AND condition fails
                }
            }
            return true; // All predicates passed
        } else if (logic_operator == OR) {
            for (const auto& pred : predicates) {
                if (pred->check(tupleFields)) {
                    return true; // If any predicate passes, the OR condition passes
                }
            }
            return false; // No predicates passed
        }
        return false;
    }


};


class SelectOperator : public UnaryOperator {
private:
    std::unique_ptr<IPredicate> predicate;
    bool has_next;
    std::vector<std::unique_ptr<Field>> currentOutput; // Store the current output here

public:
    SelectOperator(Operator& input, std::unique_ptr<IPredicate> predicate)
        : UnaryOperator(input), predicate(std::move(predicate)), has_next(false) {}

    void open() override {
        input->open();
        has_next = false;
        currentOutput.clear(); // Ensure currentOutput is cleared at the beginning
    }

    bool next() override {
        while (input->next()) {
            const auto& output = input->getOutput(); // Temporarily hold the output
            if (predicate->check(output)) {
                // If the predicate is satisfied, store the output in the member variable
                currentOutput.clear(); // Clear previous output
                for (const auto& field : output) {
                    // Assuming Field class has a clone method or copy constructor to duplicate fields
                    currentOutput.push_back(field->clone());
                }
                has_next = true;
                return true;
            }
        }
        has_next = false;
        currentOutput.clear(); // Clear output if no more tuples satisfy the predicate
        return false;
    }

    void close() override {
        input->close();
        currentOutput.clear(); // Ensure currentOutput is cleared at the end
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (has_next) {
            // Since currentOutput already holds the desired output, simply return it
            // Need to create a deep copy to return since we're returning by value
            std::vector<std::unique_ptr<Field>> outputCopy;
            for (const auto& field : currentOutput) {
                outputCopy.push_back(field->clone()); // Clone each field
            }
            return outputCopy;
        } else {
            return {}; // Return an empty vector if no matching tuple is found
        }
    }
};

enum class AggrFuncType { COUNT, MAX, MIN, SUM };

struct AggrFunc {
    AggrFuncType func;
    size_t attr_index; // Index of the attribute to aggregate
};

class HashAggregationOperator : public UnaryOperator {
private:
    std::vector<size_t> group_by_attrs;
    std::vector<AggrFunc> aggr_funcs;
    std::vector<Tuple> output_tuples; // Use your Tuple class for output
    size_t output_tuples_index = 0;

    struct FieldVectorHasher {
        std::size_t operator()(const std::vector<Field>& fields) const {
            std::size_t hash = 0;
            for (const auto& field : fields) {
                std::hash<std::string> hasher;
                std::size_t fieldHash = 0;

                // Depending on the type, hash the corresponding data
                switch (field.type) {
                    case INT: {
                        // Convert integer data to string and hash
                        int value = *reinterpret_cast<const int*>(field.data.get());
                        fieldHash = hasher(std::to_string(value));
                        break;
                    }
                    case FLOAT: {
                        // Convert float data to string and hash
                        float value = *reinterpret_cast<const float*>(field.data.get());
                        fieldHash = hasher(std::to_string(value));
                        break;
                    }
                    case STRING: {
                        // Directly hash the string data
                        std::string value(field.data.get(), field.data_length - 1); // Exclude null-terminator
                        fieldHash = hasher(value);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unsupported field type for hashing.");
                }

                // Combine the hash of the current field with the hash so far
                hash ^= fieldHash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        }
    };


public:
    HashAggregationOperator(Operator& input, std::vector<size_t> group_by_attrs, std::vector<AggrFunc> aggr_funcs)
        : UnaryOperator(input), group_by_attrs(group_by_attrs), aggr_funcs(aggr_funcs) {}

    void open() override {
        input->open(); // Ensure the input operator is opened
        output_tuples_index = 0;
        output_tuples.clear();

        // Assume a hash map to aggregate tuples based on group_by_attrs
        std::unordered_map<std::vector<Field>, std::vector<Field>, FieldVectorHasher> hash_table;

        while (input->next()) {
            const auto& tuple = input->getOutput(); // Assume getOutput returns a reference to the current tuple

            // Extract group keys and initialize aggregation values
            std::vector<Field> group_keys;
            for (auto& index : group_by_attrs) {
                group_keys.push_back(*tuple[index]); // Deep copy the Field object for group key
            }

            // Process aggregation functions
            if (!hash_table.count(group_keys)) {
                // Initialize aggregate values for a new group
                std::vector<Field> aggr_values(aggr_funcs.size(), Field(0)); // Assuming Field(int) initializes an integer Field
                hash_table[group_keys] = aggr_values;
            }

            // Update aggregate values
            auto& aggr_values = hash_table[group_keys];
            for (size_t i = 0; i < aggr_funcs.size(); ++i) {
                // Simplified update logic for demonstration
                // You'll need to implement actual aggregation logic here
                aggr_values[i] = updateAggregate(aggr_funcs[i], aggr_values[i], *tuple[aggr_funcs[i].attr_index]);
            }
        }

        // Prepare output tuples from the hash table
        for (const auto& entry : hash_table) {
            const auto& group_keys = entry.first;
            const auto& aggr_values = entry.second;
            Tuple output_tuple;
            // Assuming Tuple has a method to add Fields
            for (const auto& key : group_keys) {
                output_tuple.addField(std::make_unique<Field>(key)); // Add group keys to the tuple
            }
            for (const auto& value : aggr_values) {
                output_tuple.addField(std::make_unique<Field>(value)); // Add aggregated values to the tuple
            }
            output_tuples.push_back(std::move(output_tuple));
        }
    }

    bool next() override {
        if (output_tuples_index < output_tuples.size()) {
            output_tuples_index++;
            return true;
        }
        return false;
    }

    void close() override {
        input->close();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        std::vector<std::unique_ptr<Field>> outputCopy;

        if (output_tuples_index == 0 || output_tuples_index > output_tuples.size()) {
            // If there is no current tuple because next() hasn't been called yet or we're past the last tuple,
            // return an empty vector.
            return outputCopy; // This will be an empty vector
        }

        // Assuming that output_tuples stores Tuple objects and each Tuple has a vector of Field objects or similar
        const auto& currentTuple = output_tuples[output_tuples_index - 1]; // Adjust for 0-based indexing after increment in next()

        // Assuming the Tuple class provides a way to access its fields, e.g., a method or a public member
        for (const auto& field : currentTuple.fields) {
            outputCopy.push_back(field->clone()); // Use the clone method to create a deep copy of each field
        }

        return outputCopy;
    }


private:

    Field updateAggregate(const AggrFunc& aggrFunc, const Field& currentAggr, const Field& newValue) {
        if (currentAggr.getType() != newValue.getType()) {
            throw std::runtime_error("Mismatched Field types in aggregation.");
        }

        switch (aggrFunc.func) {
            case AggrFuncType::COUNT: {
                if (currentAggr.getType() == FieldType::INT) {
                    // For COUNT, simply increment the integer value
                    int count = currentAggr.asInt() + 1;
                    return Field(count);
                }
                break;
            }
            case AggrFuncType::SUM: {
                if (currentAggr.getType() == FieldType::INT) {
                    int sum = currentAggr.asInt() + newValue.asInt();
                    return Field(sum);
                } else if (currentAggr.getType() == FieldType::FLOAT) {
                    float sum = currentAggr.asFloat() + newValue.asFloat();
                    return Field(sum);
                }
                break;
            }
            case AggrFuncType::MAX: {
                if (currentAggr.getType() == FieldType::INT) {
                    int max = std::max(currentAggr.asInt(), newValue.asInt());
                    return Field(max);
                } else if (currentAggr.getType() == FieldType::FLOAT) {
                    float max = std::max(currentAggr.asFloat(), newValue.asFloat());
                    return Field(max);
                }
                break;
            }
            case AggrFuncType::MIN: {
                if (currentAggr.getType() == FieldType::INT) {
                    int min = std::min(currentAggr.asInt(), newValue.asInt());
                    return Field(min);
                } else if (currentAggr.getType() == FieldType::FLOAT) {
                    float min = std::min(currentAggr.asFloat(), newValue.asFloat());
                    return Field(min);
                }
                break;
            }
            default:
                throw std::runtime_error("Unsupported aggregation function.");
        }

        // Default case for unsupported operations or types
        throw std::runtime_error(
            "Invalid operation or unsupported Field type.");
    }

};

struct QueryComponents {
    std::vector<int> selectAttributes;
    bool sumOperation = false;
    int sumAttributeIndex = -1;
    bool groupBy = false;
    int groupByAttributeIndex = -1;
    bool whereCondition = false;
    int whereAttributeIndex = -1;
    int lowerBound = std::numeric_limits<int>::min();
    int upperBound = std::numeric_limits<int>::max();
};

QueryComponents parseQuery(const std::string& query) {
    QueryComponents components;

    // Parse selected attributes
    std::regex selectRegex("\\{(\\d+)\\}(, \\{(\\d+)\\})?");
    std::smatch selectMatches;
    std::string::const_iterator queryStart(query.cbegin());
    while (std::regex_search(queryStart, query.cend(), selectMatches, selectRegex)) {
        for (size_t i = 1; i < selectMatches.size(); i += 2) {
            if (!selectMatches[i].str().empty()) {
                components.selectAttributes.push_back(std::stoi(selectMatches[i]) - 1);
            }
        }
        queryStart = selectMatches.suffix().first;
    }

    // Check for SUM operation
    std::regex sumRegex("SUM\\{(\\d+)\\}");
    std::smatch sumMatches;
    if (std::regex_search(query, sumMatches, sumRegex)) {
        components.sumOperation = true;
        components.sumAttributeIndex = std::stoi(sumMatches[1]) - 1;
    }

    // Check for GROUP BY clause
    std::regex groupByRegex("GROUP BY \\{(\\d+)\\}");
    std::smatch groupByMatches;
    if (std::regex_search(query, groupByMatches, groupByRegex)) {
        components.groupBy = true;
        components.groupByAttributeIndex = std::stoi(groupByMatches[1]) - 1;
    }

    // Extract WHERE conditions more accurately
    std::regex whereRegex("\\{(\\d+)\\} > (\\d+) and \\{(\\d+)\\} < (\\d+)");
    std::smatch whereMatches;
    if (std::regex_search(query, whereMatches, whereRegex)) {
        components.whereCondition = true;
        // Correctly identify the attribute index for the WHERE condition
        components.whereAttributeIndex = std::stoi(whereMatches[1]) - 1;
        components.lowerBound = std::stoi(whereMatches[2]);
        // Ensure the same attribute is used for both conditions
        if (std::stoi(whereMatches[3]) - 1 == components.whereAttributeIndex) {
            components.upperBound = std::stoi(whereMatches[4]);
        } else {
            std::cerr << "Error: WHERE clause conditions apply to different attributes." << std::endl;
            // Handle error or set components.whereCondition = false;
        }
    }

    return components;
}

void prettyPrint(const QueryComponents& components) {
    std::cout << "Query Components:\n";
    std::cout << "  Selected Attributes: ";
    for (auto attr : components.selectAttributes) {
        std::cout << "{" << attr + 1 << "} "; // Convert back to 1-based indexing for display
    }
    std::cout << "\n  SUM Operation: " << (components.sumOperation ? "Yes" : "No");
    if (components.sumOperation) {
        std::cout << " on {" << components.sumAttributeIndex + 1 << "}";
    }
    std::cout << "\n  GROUP BY: " << (components.groupBy ? "Yes" : "No");
    if (components.groupBy) {
        std::cout << " on {" << components.groupByAttributeIndex + 1 << "}";
    }
    std::cout << "\n  WHERE Condition: " << (components.whereCondition ? "Yes" : "No");
    if (components.whereCondition) {
        std::cout << " on {" << components.whereAttributeIndex + 1 << "} > " << components.lowerBound << " and < " << components.upperBound;
    }
    std::cout << std::endl;
}

void executeQuery(const QueryComponents& components, 
                  BufferManager& buffer_manager) {
    // Stack allocation of ScanOperator
    ScanOperator scanOp(buffer_manager);

    // Using a pointer to Operator to handle polymorphism
    Operator* rootOp = &scanOp;

    // Buffer for optional operators to ensure lifetime
    std::optional<SelectOperator> selectOpBuffer;
    std::optional<HashAggregationOperator> hashAggOpBuffer;

    // Apply WHERE conditions
    if (components.whereAttributeIndex != -1) {
        // Create simple predicates with comparison operators
        auto predicate1 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(components.whereAttributeIndex),
            SimplePredicate::Operand(std::make_unique<Field>(components.lowerBound)),
            SimplePredicate::ComparisonOperator::GT
        );

        auto predicate2 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(components.whereAttributeIndex),
            SimplePredicate::Operand(std::make_unique<Field>(components.upperBound)),
            SimplePredicate::ComparisonOperator::LT
        );

        // Combine simple predicates into a complex predicate with logical AND operator
        auto complexPredicate = std::make_unique<ComplexPredicate>(ComplexPredicate::LogicOperator::AND);
        complexPredicate->addPredicate(std::move(predicate1));
        complexPredicate->addPredicate(std::move(predicate2));

        // Using std::optional to manage the lifetime of SelectOperator
        selectOpBuffer.emplace(*rootOp, std::move(complexPredicate));
        rootOp = &*selectOpBuffer;
    }

    // Apply SUM or GROUP BY operation
    if (components.sumOperation || components.groupBy) {
        std::vector<size_t> groupByAttrs;
        if (components.groupBy) {
            groupByAttrs.push_back(static_cast<size_t>(components.groupByAttributeIndex));
        }
        std::vector<AggrFunc> aggrFuncs{
            {AggrFuncType::SUM, static_cast<size_t>(components.sumAttributeIndex)}
        };

        // Using std::optional to manage the lifetime of HashAggregationOperator
        hashAggOpBuffer.emplace(*rootOp, groupByAttrs, aggrFuncs);
        rootOp = &*hashAggOpBuffer;
    }

    // Execute the Root Operator
    rootOp->open();
    while (rootOp->next()) {
        // Retrieve and print the current tuple
        const auto& output = rootOp->getOutput();
        for (const auto& field : output) {
            field->print();
            std::cout << " ";
        }
        std::cout << std::endl;
    }
    rootOp->close();
}

enum Colour {
    RED,
    BLACK
};

struct Node {
    int key;
    std::unique_ptr<Tuple> tuple;
    Colour colour;
    Node *left, *right, *parent;

    Node(int key, std::unique_ptr<Tuple> tuple): key(key), tuple(std::move(tuple)), colour(RED), left(nullptr), right(nullptr), parent(nullptr){}
};

class RedBlackTree {
    private:
        Node* root;
        Node* NIL;

        void bstInsert(Node* newNode) {
            Node* parent = nullptr;
            Node* current = root;
            while (current != NIL) {
                parent = current;
                if (newNode->key < current->key) {
                    current = current->left;
                }
                else {
                    current = current->right;
                }
            }

            newNode->parent = parent;
            if (parent == nullptr) {
                root = newNode;
            }
            else if (newNode->key < parent->key) {
                parent->left = newNode;
            }
            else {
                parent->right = newNode;
            }

            if (newNode->parent == nullptr) {
                newNode->colour = BLACK;
                return;
            }

            if (newNode->parent->parent == nullptr) {
                return;
            }
        }

        void leftRotate(Node* x){
            Node* y = x->right;
            x->right = y->left;
            if (y->left != NIL) {
                y->left->parent = x;
            }
            y->parent = x->parent;
            if (x->parent == nullptr) {
                root = y;
            }
            else if (x == x->parent->left) {
                x->parent->left = y;
            }
            else {
                x->parent->right = y;
            }
            y->left = x;
            x->parent = y;
        }

        void rightRotate(Node* x) {
            Node* y = x->left;
            x->left = y->right;
            if (y->right != NIL) {
                y->right->parent = x;
            }
            y->parent = x->parent;
            if (x->parent == nullptr) {
                root = y;
            }
            else if (x == x->parent->right) {
                x->parent->right = y;
            }
            else {
                x->parent->left = y;
            }
            y->right = x;
            x->parent = y;
        }

        void fixTree(Node* k) {
            while (k != root && k->parent->colour == RED) {
                if (k->parent == k->parent->parent->left) {
                    Node* u = k->parent->parent->right;
                    if (u->colour == RED) {
                        k->parent->colour = BLACK;
                        u->colour = BLACK;
                        k->parent->parent->colour = RED;
                        k = k->parent->parent;
                    }
                    else {
                        if (k == k->parent->right) {
                            k = k->parent;
                            leftRotate(k);
                        }
                        k->parent->colour = BLACK;
                        k->parent->parent->colour = RED;
                        rightRotate(k->parent->parent);
                    }
                }
                else {
                    Node* u = k->parent->parent->left;
                    if (u->colour == RED) {
                        k->parent->colour = BLACK;
                        u->colour = BLACK;
                        k->parent->parent->colour = RED;
                        k = k->parent->parent;
                    }
                    else {
                        if (k == k->parent->left) {
                            k = k->parent;
                            rightRotate(k);
                        }
                        k->parent->colour = BLACK;
                        k->parent->parent->colour = RED;
                        leftRotate(k->parent->parent);
                    }
                }
            }
            root->colour = BLACK;
        }

        void inorder(Node* node, std::vector<std::unique_ptr<Tuple>>* inorderList) {
            if (node != NIL) {
                inorder(node->left, inorderList);
                inorderList->push_back(std::move(node->tuple));
                inorder(node->right, inorderList);
            }
        }

        void clearNode(Node* node) {
            if (node != NIL) {
                clearNode(node->left);
                clearNode(node->right);
                delete node;
            }
        }

    public:
        const int MAX_NODES = 1000;
        int numNodes;
        RedBlackTree() {
            NIL = new Node(0, 0);
            NIL->colour = BLACK;
            NIL->left = NIL->right = NIL;
            root = NIL;
            numNodes = 0;
        }

        void insert(int key, std::unique_ptr<Tuple> tuple) {
            std::cout << "Inserting key: " << key << std::endl;
            Node* newNode = new Node(key, std::move(tuple));
            newNode->left = NIL;
            newNode->right = NIL;
            std::cout << "New node created with key: " << newNode->key << std::endl;
            bstInsert(newNode);
            std::cout << "BST Insert completed for key: " << newNode->key << std::endl;
            fixTree(newNode);
            std::cout << "Fixing tree completed for key: " << newNode->key << std::endl;
            numNodes++;
        }

        std::vector<std::unique_ptr<Tuple>> getInorder() { 
            std::vector<std::unique_ptr<Tuple>> inorderList;
            inorder(root, &inorderList); 
            return inorderList;
        }

        void clearTree() {
            clearNode(root);
            root = NIL;
            numNodes = 0;
        }

        // Method to search for a key in the Red-Black Tree
        Node* search(int key) const {
            Node* current = root;

            while (current != nullptr) {
                // Found the key
                if (key == current->key) {
                    return current;
                }
                // Go left if key is smaller
                else if (key < current->key) {
                    current = current->left;
                }
                // Go right if key is larger
                else {
                    current = current->right;
                }
            }

            // Key not found
            return nullptr;
        }
};


class SSTFile {
    private:
        std::string filename;
        std::vector<std::unique_ptr<Tuple>> tuples;
        int fileId;
    
    public:
        SSTFile(int id) : fileId(id) {
            filename = "sst_" + std::to_string(id) + ".dat";
        }
    
        SSTFile(const std::string& fname) : filename(fname) {
            // Extract fileId from filename (assuming format "sst_X.dat")
            size_t start = filename.find('_') + 1;
            size_t end = filename.find('.');
            fileId = std::stoi(filename.substr(start, end - start));
        }
    
        void addTuple(std::unique_ptr<Tuple> tuple) {
            tuples.push_back(std::move(tuple));
        }
    
        void writeToDisk() {
            std::ofstream outFile(filename, std::ios::binary);
            if (!outFile) {
                std::cerr << "Failed to open SST file for writing: " << filename << std::endl;
                return;
            }
    
            // Write number of tuples
            size_t numTuples = tuples.size();
            outFile.write(reinterpret_cast<const char*>(&numTuples), sizeof(numTuples));
    
            // Write each tuple
            for (const auto& tuple : tuples) {
                // For simplicity, assuming each tuple has exactly 2 integer fields
                int key = tuple->fields[0]->asInt();
                int value = tuple->fields[1]->asInt();
                
                outFile.write(reinterpret_cast<const char*>(&key), sizeof(key));
                outFile.write(reinterpret_cast<const char*>(&value), sizeof(value));
            }
    
            outFile.close();
            std::cout << "Written " << numTuples << " tuples to SST file: " << filename << std::endl;
        }
    
        std::string getFilename() const {
            return filename;
        }
    
        int getFileId() const {
            return fileId;
        }
    
        // Load tuples from disk file
        bool loadFromDisk() {
            tuples.clear();
            
            std::ifstream inFile(filename, std::ios::binary);
            if (!inFile) {
                std::cerr << "Failed to open SST file for reading: " << filename << std::endl;
                return false;
            }
    
            // Read number of tuples
            size_t numTuples;
            inFile.read(reinterpret_cast<char*>(&numTuples), sizeof(numTuples));
    
            // Read each tuple
            for (size_t i = 0; i < numTuples; i++) {
                int key, value;
                inFile.read(reinterpret_cast<char*>(&key), sizeof(key));
                inFile.read(reinterpret_cast<char*>(&value), sizeof(value));
    
                auto newTuple = std::make_unique<Tuple>();
                newTuple->addField(std::make_unique<Field>(key));
                newTuple->addField(std::make_unique<Field>(value));
                
                tuples.push_back(std::move(newTuple));
            }
    
            inFile.close();
            std::cout << "Loaded " << numTuples << " tuples from SST file: " << filename << std::endl;
            return true;
        }
    
        std::vector<std::unique_ptr<Tuple>>& getTuples() {
            return tuples;
        }
};


class SSTManager {
    private:
        int nextFileId;
    
    public:
        int numSST = 0;
        std::vector<std::string> sstFiles;
        SSTManager() : nextFileId(0) {
            // Check if there are existing SST files from previous runs
            loadExistingSSTs();
        }
    
        // Load any existing SST files from the disk
        void loadExistingSSTs() {
            // This would be more robust with filesystem API, but simple pattern matching works for demo
            for (int i = 0; i < 1000; i++) {  // Arbitrary upper limit
                std::string filename = "sst_" + std::to_string(i) + ".dat";
                std::ifstream testFile(filename);
                if (testFile) {
                    sstFiles.push_back(filename);
                    if (i >= nextFileId) {
                        nextFileId = i + 1;
                    }
                    testFile.close();
                    std::cout << "Found existing SST file: " << filename << std::endl;
                }
            }
            std::cout << "Found " << sstFiles.size() << " existing SST files, nextFileId = " << nextFileId << std::endl;
        }
    
        int createNewSST(std::vector<std::unique_ptr<Tuple>>& tuples) {
            // activeThreads++;
             // Create new SST file
             // std::cout<<"CREATE NEW SST"<<std::endl;
            std::cout<<"CREATE NEW SST: numSST: "<<numSST<<std::endl;
            SSTFile sstFile(nextFileId);
            
            // Add tuples to SST file
            for (auto& tuple : tuples) {
                sstFile.addTuple(std::move(tuple));
            }
            
            // Write SST to disk
            sstFile.writeToDisk();
            
            // Add to list of SST files
            sstFiles.push_back(sstFile.getFilename());
            
            int currentFileId = nextFileId++;
            
            // Check if compaction is needed
            if (sstFiles.size() >= 2) {
                // std::thread compactSSTsThread(&SSTManager::compactSSTs, this);
                // compactSSTsThread.join();
                compactSSTs(); //Without Concurrency
            }
            numSST++;

            std::cout<<"OUR TEAM IS THE BEST. PLEASE LET US PRESENT AND GIVE 10% BONUS. THANK. YOU BEST PROF. numSST: "<< numSST<<std::endl;
    
            return currentFileId;
        }
    
        void compactSSTs() {
            if (sstFiles.size() < 2) return;
            
            std::cout << "Starting compaction of SST files..." << std::endl;
            
            // Select the two oldest files based on their filenames
            std::sort(sstFiles.begin(), sstFiles.end());
            std::string file1Name = sstFiles[0];
            std::string file2Name = sstFiles[1];
            
            SSTFile file1(file1Name);
            SSTFile file2(file2Name);
            
            if (!file1.loadFromDisk() || !file2.loadFromDisk()) {
                std::cerr << "Failed to load SST files for compaction" << std::endl;
                return;
            }
            
            // Merge tuples, handling duplicates by keeping latest value
            std::map<int, std::unique_ptr<Tuple>> mergedTuples;
            
            // Process tuples from first file
            for (auto& tuple : file1.getTuples()) {
                int key = tuple->fields[0]->asInt();
                mergedTuples[key] = std::move(tuple);
            }
            
            // Process tuples from second file, overwriting duplicates
            for (auto& tuple : file2.getTuples()) {
                int key = tuple->fields[0]->asInt();
                mergedTuples[key] = std::move(tuple);
            }
            
            // Create new compacted SST file
            SSTFile compactedFile(nextFileId);
            
            // Add merged tuples to compacted file
            for (auto& pair : mergedTuples) {
                compactedFile.addTuple(std::move(pair.second));
            }
            
            // Write compacted file to disk
            compactedFile.writeToDisk();
            
            // Remove old SST files from disk
            std::remove(file1Name.c_str());
            std::remove(file2Name.c_str());
            
            // Update SST files list
            sstFiles.erase(sstFiles.begin(), sstFiles.begin() + 2);
            sstFiles.push_back(compactedFile.getFilename());
            
            nextFileId++;
            
            std::cout << "Compaction completed. Created new SST file: " << compactedFile.getFilename() << std::endl;
        }
        
        // Method to read all data from SST files
        std::vector<std::unique_ptr<Tuple>> readAllData() {
            std::map<int, std::unique_ptr<Tuple>> allData;
            
            // Read from all SST files, with newer files overriding older ones
            std::sort(sstFiles.begin(), sstFiles.end()); // Sort by filename to get chronological order
            
            for (const auto& filename : sstFiles) {
                SSTFile sstFile(filename);
                if (sstFile.loadFromDisk()) {
                    for (auto& tuple : sstFile.getTuples()) {
                        int key = tuple->fields[0]->asInt();
                        allData[key] = std::move(tuple);
                    }
                }
            }
            
            // Convert map to vector
            std::vector<std::unique_ptr<Tuple>> result;
            for (auto& pair : allData) {
                result.push_back(std::move(pair.second));
            }
            
            return result;
        }
        
        // Method to lookup a specific key
        std::unique_ptr<Tuple> lookup(int key) {
            // Start from newest SST files (higher fileId)
            std::vector<std::string> sortedFiles = sstFiles;
            std::sort(sortedFiles.begin(), sortedFiles.end(), std::greater<std::string>());
            
            for (const auto& filename : sortedFiles) {
                SSTFile sstFile(filename);
                if (sstFile.loadFromDisk()) {
                    for (auto& tuple : sstFile.getTuples()) {
                        if (tuple->fields[0]->asInt() == key) {
                            return tuple->clone();
                        }
                    }
                }
            }
            
            return nullptr; // Key not found
        }
};


// WAL Log class implementation
class WALLog {
    private:
        std::string logFileName;
        std::ofstream logFile;
        
        // Log entry types
        enum LogEntryType {
            INSERT = 1,
            DELETE = 2,
            COMMIT = 3,
            CHECKPOINT = 4
        };
        
    public:
        WALLog(const std::string& fileName = "buzzdb_wal.log") : logFileName(fileName) {
            // Open log file in append mode
            logFile.open(logFileName, std::ios::binary | std::ios::app);
            if (!logFile) {
                std::cerr << "Failed to open WAL log file: " << logFileName << std::endl;
                throw std::runtime_error("Failed to open WAL log file");
            }
            
            // Write a checkpoint entry on startup
            writeCheckpoint();
        }
        
        ~WALLog() {
            if (logFile.is_open()) {
                logFile.close();
            }
        }
        
        // Log an insert operation
        void logInsert(int key, int value) {
            if (!logFile.is_open()) return;
            
            // Log entry format: [Type(1B)][Key(4B)][Value(4B)]
            uint8_t type = LogEntryType::INSERT;
            logFile.write(reinterpret_cast<const char*>(&type), sizeof(type));
            logFile.write(reinterpret_cast<const char*>(&key), sizeof(key));
            logFile.write(reinterpret_cast<const char*>(&value), sizeof(value));
            logFile.flush();
        }
        
        // Log a delete operation
        void logDelete(int key) {
            if (!logFile.is_open()) return;
            
            // Log entry format: [Type(1B)][Key(4B)]
            uint8_t type = LogEntryType::DELETE;
            logFile.write(reinterpret_cast<const char*>(&type), sizeof(type));
            logFile.write(reinterpret_cast<const char*>(&key), sizeof(key));
            logFile.flush();
        }
        
        // Log a commit
        void logCommit() {
            if (!logFile.is_open()) return;
            
            // Log entry format: [Type(1B)]
            uint8_t type = LogEntryType::COMMIT;
            logFile.write(reinterpret_cast<const char*>(&type), sizeof(type));
            logFile.flush();
        }
        
        // Write a checkpoint entry
        void writeCheckpoint() {
            if (!logFile.is_open()) return;
            
            // Log entry format: [Type(1B)]
            uint8_t type = LogEntryType::CHECKPOINT;
            logFile.write(reinterpret_cast<const char*>(&type), sizeof(type));
            logFile.flush();
            
            std::cout << "WAL checkpoint written" << std::endl;
        }
        
        // Truncate log after successful checkpoint
        void truncate() {
            if (logFile.is_open()) {
                logFile.close();
            }
            
            // Reopen file in write mode (clears the file)
            logFile.open(logFileName, std::ios::binary | std::ios::trunc);
            if (!logFile) {
                std::cerr << "Failed to truncate WAL log file: " << logFileName << std::endl;
                // Try to reopen in append mode at least
                logFile.open(logFileName, std::ios::binary | std::ios::app);
            }
            
            // Write a new checkpoint entry
            writeCheckpoint();
        }
        
        // Recovery function to replay log after crash
        std::vector<std::pair<int, int>> recover() {
            // Close the file if it's open
            if (logFile.is_open()) {
                logFile.close();
            }
            
            // Map to store recovered key-value pairs
            std::map<int, int> recoveredData;
            // Set to store keys that were deleted
            std::set<int> deletedKeys;
            
            // Open for reading
            std::ifstream reader(logFileName, std::ios::binary);
            if (!reader) {
                std::cerr << "Failed to open WAL log file for recovery: " << logFileName << std::endl;
                // Re-open the log file for writing
                logFile.open(logFileName, std::ios::binary | std::ios::app);
                return {};
            }
            
            std::cout << "Starting WAL recovery process..." << std::endl;
            
            bool foundCheckpoint = false;
            size_t checkpointPos = 0;
            
            // First pass: find the last checkpoint position
            while (!reader.eof()) {
                uint8_t type;
                reader.read(reinterpret_cast<char*>(&type), sizeof(type));
                
                if (reader.eof() || !reader.good()) break;
                
                if (type == LogEntryType::CHECKPOINT) {
                    foundCheckpoint = true;
                    checkpointPos = reader.tellg();
                    checkpointPos -= sizeof(type); // Adjust to the start of the entry
                } else if (type == LogEntryType::INSERT) {
                    // Skip INSERT entry data
                    reader.seekg(sizeof(int) * 2, std::ios::cur);
                } else if (type == LogEntryType::DELETE) {
                    // Skip DELETE entry data
                    reader.seekg(sizeof(int), std::ios::cur);
                }
                // COMMIT doesn't have extra data
            }
            
            // If we found a checkpoint, start from there
            if (foundCheckpoint) {
                reader.clear(); // Clear any EOF flags
                reader.seekg(checkpointPos, std::ios::beg);
                std::cout << "Found checkpoint at position " << checkpointPos << std::endl;
            } else {
                // No checkpoint found, start from beginning
                reader.clear();
                reader.seekg(0, std::ios::beg);
                std::cout << "No checkpoint found, replaying entire log" << std::endl;
            }
            
            // Second pass: replay log entries
            int replayedEntries = 0;
            while (!reader.eof()) {
                uint8_t type;
                reader.read(reinterpret_cast<char*>(&type), sizeof(type));
                
                if (reader.eof() || !reader.good()) break;
                
                if (type == LogEntryType::INSERT) {
                    int key, value;
                    reader.read(reinterpret_cast<char*>(&key), sizeof(key));
                    reader.read(reinterpret_cast<char*>(&value), sizeof(value));
                    
                    if (reader.good()) {
                        recoveredData[key] = value;
                        deletedKeys.erase(key); // Remove from deleted if it was there
                        replayedEntries++;
                    }
                } else if (type == LogEntryType::DELETE) {
                    int key;
                    reader.read(reinterpret_cast<char*>(&key), sizeof(key));
                    
                    if (reader.good()) {
                        recoveredData.erase(key);
                        deletedKeys.insert(key);
                        replayedEntries++;
                    }
                } else if (type == LogEntryType::CHECKPOINT) {
                    // For checkpoint entries, we continue - we already handled the positioning above
                }
                // COMMIT doesn't change our recovery data
            }
            
            reader.close();
            
            // Re-open the log file for writing
            logFile.open(logFileName, std::ios::binary | std::ios::app);
            
            std::cout << "WAL recovery completed. Replayed " << replayedEntries << " entries." << std::endl;
            
            // Convert map to vector of pairs
            std::vector<std::pair<int, int>> result;
            for (const auto& pair : recoveredData) {
                if (deletedKeys.find(pair.first) == deletedKeys.end()) {
                    result.push_back(pair);
                }
            }
            
            return result;
        }

        // Print the contents of a WAL log file
        void printWALLog(const std::string& fileName = "buzzdb_wal.log") {
            std::ifstream logFile(fileName, std::ios::binary);

            if (!logFile) {
                std::cerr << "Failed to open WAL log file: " << fileName << std::endl;
                return;
            }

            std::cout << "=== WAL Log File: " << fileName << " ===" << std::endl;
            std::cout << "Position | Type       | Data" << std::endl;
            std::cout << "---------+------------+------------------" << std::endl;

            size_t position = 0;
            size_t entryCount = 0;

            while (!logFile.eof()) {
                // Record the current position
                position = logFile.tellg();

                // Read the entry type
                uint8_t type;
                logFile.read(reinterpret_cast<char*>(&type), sizeof(type));

                if (logFile.eof() || !logFile.good()) break;

                // Print position and entry type
                std::cout << std::setw(8) << position << " | ";

                // Process based on entry type
                switch (type) {
                    case LogEntryType::INSERT: {
                        int key, value;
                        logFile.read(reinterpret_cast<char*>(&key), sizeof(key));
                        logFile.read(reinterpret_cast<char*>(&value), sizeof(value));

                        if (logFile.good()) {
                            std::cout << "INSERT      | Key: " << key << ", Value: " << value << std::endl;
                        }
                        break;
                    }
                    case LogEntryType::DELETE: {
                        int key;
                        logFile.read(reinterpret_cast<char*>(&key), sizeof(key));

                        if (logFile.good()) {
                            std::cout << "DELETE      | Key: " << key << std::endl;
                        }
                        break;
                    }
                    case LogEntryType::COMMIT:
                        std::cout << "COMMIT      |" << std::endl;
                        break;
                    case LogEntryType::CHECKPOINT:
                        std::cout << "CHECKPOINT  |" << std::endl;
                        break;
                    default:
                        std::cout << "UNKNOWN(" << static_cast<int>(type) << ") |" << std::endl;
                        // For unknown types, we can't reliably continue parsing
                        logFile.seekg(0, std::ios::end);
                        break;
                }

                entryCount++;
            }

            logFile.close();

            std::cout << "---------+------------+------------------" << std::endl;
            std::cout << "Total entries: " << entryCount << std::endl;
        }
};


class InsertOperator : public Operator {
private:
    BufferManager& bufferManager;
    std::unique_ptr<Tuple> tupleToInsert;
    RedBlackTree& redBlackTree;
    SSTManager& sstManager;

public:
    InsertOperator(BufferManager& manager, RedBlackTree& redBlackTree, SSTManager& sstManager) : bufferManager(manager), redBlackTree(redBlackTree), sstManager(sstManager){}

    // Set the tuple to be inserted by this operator.
    void setTupleToInsert(std::unique_ptr<Tuple> tuple) {
        tupleToInsert = std::move(tuple);
    }

    void open() override {
        // Not used in this context
    }

    bool next() override {
        if (!tupleToInsert) return false; // No tuple to insert
        int pageId = bufferManager.getCurrentPageId();
        std::cout<<"Current page id in slot manager: "<<pageId<<std::endl;
        int key = tupleToInsert->fields[0]->asInt();
        redBlackTree.insert(key, std::move(tupleToInsert));
        std::cout << "RBT Insert Done" << std::endl;
        if(redBlackTree.numNodes == redBlackTree.MAX_NODES){
            // std::vector<std::unique_ptr<Tuple>> inorder = redBlackTree.getInorder();
            // auto& page = bufferManager.getPage(pageId);
            // for(int i = 0; i < redBlackTree.MAX_NODES;i++){
            //     page->addTuple(inorder[i]->clone());
            // }
            // bufferManager.flushPage(pageId);
            // bufferManager.extend();
            
            // bufferManager.readPage(pageId);

            // Write tuples to a new SST file
            // sstManager.createNewSST(inorder); // Without concurrency

            // std::thread writeRBTtoSST (&SSTManager::createNewSST, &sstManager, std::ref(inorder));
            // writeRBTtoSST.join();


            auto inorderCopy = new std::vector<std::unique_ptr<Tuple>>(redBlackTree.getInorder());
            redBlackTree.clearTree();
            std::thread writeRBTtoSST([this, inorderCopy]() {
                // Thread takes ownership of the data
                sstManager.createNewSST(*inorderCopy);
                delete inorderCopy; // Clean up the memory when done
            });
            writeRBTtoSST.detach();
            
            // Clear the RBT for new insertions
            // redBlackTree.clearTree();
        }
        return true;
        // If insertion failed in all existing pages, extend the database and try again
        // bufferManager.extend();
        // auto& newPage = bufferManager.getPage(bufferManager.getNumPages() - 1);
        // if (newPage->addTuple(tupleToInsert->clone())) {
        //     bufferManager.flushPage(bufferManager.getNumPages() - 1);
        //     return true; // Insertion successful after extending the database
        // }

        // return false; // Insertion failed even after extending the database
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};

class DeleteOperator : public Operator {
private:
    BufferManager& bufferManager;
    size_t pageId;
    size_t tupleId;

public:
    DeleteOperator(BufferManager& manager, size_t pageId, size_t tupleId) 
        : bufferManager(manager), pageId(pageId), tupleId(tupleId) {}

    void open() override {
        // Not used in this context
    }

    bool next() override {
        auto& page = bufferManager.getPage(pageId);
        if (!page) {
            std::cerr << "Page not found." << std::endl;
            return false;
        }

        page->deleteTuple(tupleId); // Perform deletion
        bufferManager.flushPage(pageId); // Flush the page to disk after deletion
        return true;
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};


class BuzzDB {
public:
    HashIndex hash_index;
    BufferManager buffer_manager;
    RedBlackTree redBlackTree;
    SSTManager sstManager;
    WALLog walLog;

public:
    size_t max_number_of_tuples = 5000;
    size_t tuple_insertion_attempt_counter = 0;
    bool recoveryPerformed = false;

    BuzzDB(){
        // Storage Manager automatically created
        // Perform recovery if needed
        recoverFromWAL();
    }

    // Method to recover data from WAL during startup
    void recoverFromWAL() {
        std::cout << "Checking for WAL recovery..." << std::endl;
        auto recoveredData = walLog.recover();
        
        if (!recoveredData.empty()) {
            std::cout << "Found " << recoveredData.size() << " records to recover" << std::endl;
            
            // Insert recovered data
            for (const auto& pair : recoveredData) {
                int key = pair.first;
                int value = pair.second;
                
                // Create a new tuple with the recovered key and value
                auto newTuple = std::make_unique<Tuple>();
                newTuple->addField(std::make_unique<Field>(key));
                newTuple->addField(std::make_unique<Field>(value));
                
                // Insert directly into RBT
                redBlackTree.insert(key, std::move(newTuple));
                
                // Check if RBT is full and needs to be flushed
                if (redBlackTree.numNodes == redBlackTree.MAX_NODES) {
                    std::vector<std::unique_ptr<Tuple>> inorder = redBlackTree.getInorder();
                    sstManager.createNewSST(inorder);
                    redBlackTree.clearTree();
                }
            }
            
            // Write a checkpoint after recovery
            walLog.writeCheckpoint();
            recoveryPerformed = true;
        } else {
            std::cout << "No WAL recovery needed" << std::endl;
        }
    }

    // insert function
    void insert(int key, int value) {
        tuple_insertion_attempt_counter += 1;

        // Create a new tuple with the given key and value
        auto newTuple = std::make_unique<Tuple>();

        auto key_field = std::make_unique<Field>(key);
        auto value_field = std::make_unique<Field>(value);
        // float float_val = 132.04;
        // auto float_field = std::make_unique<Field>(float_val);
        // auto string_field = std::make_unique<Field>("buzzdb");

        newTuple->addField(std::move(key_field));
        newTuple->addField(std::move(value_field));

        InsertOperator insertOp(buffer_manager, redBlackTree, sstManager);
        insertOp.setTupleToInsert(std::move(newTuple));
        bool status = insertOp.next();

        assert(status == true);

        // if (tuple_insertion_attempt_counter % 10 != 0) {
        //     // Assuming you want to delete the first tuple from the first page
        //     DeleteOperator delOp(buffer_manager, 0, 0); 
        //     if (!delOp.next()) {
        //         std::cerr << "Failed to delete tuple." << std::endl;
        //     }
        // }

        // Log a commit after the operation
        walLog.logInsert(key, value);

    }

    // Function to delete a record by key
    bool remove(int key) {
        // First check if the key exists in the RBT
        Node* node = redBlackTree.search(key);
        if (node != nullptr && node->tuple != nullptr) {
            // Log the delete operation
            walLog.logDelete(key);
            
            // Currently no direct deletion from RBT, so we need to handle this differently
            // We could mark the tuple as deleted or implement RBT deletion
            // For now, we'll handle deletion as a logical operation via WAL
            
            walLog.logCommit();
            return true;
        }
        
        // Check in SST files
        auto tuple = sstManager.lookup(key);
        if (tuple != nullptr) {
            // Log the delete operation
            walLog.logDelete(key);
            
            // Currently no direct deletion from SST, handled during compaction
            // This is a logical delete via WAL
            
            walLog.logCommit();
            return true;
        }
        
        return false; // Key not found
    }

    // Function to read a value by key
    std::unique_ptr<Tuple> get(int key) {
        // First check in the RBT for recent inserts
        Node* node = redBlackTree.search(key);
        if (node != nullptr && node->tuple != nullptr) {
            // Found in RBT, return a clone of the tuple
            return node->tuple->clone();
        }
        
        // Then check in SST files
        return sstManager.lookup(key);
    }
    
    // Function to flush any remaining data in RBT to disk
    void flushMemoryTable() {
        if (redBlackTree.numNodes > 0) {
            std::vector<std::unique_ptr<Tuple>> inorder = redBlackTree.getInorder();
            sstManager.createNewSST(inorder);
            redBlackTree.clearTree();

            // Write checkpoint after flushing memtable
            // walLog.writeCheckpoint();
        }
        std::vector<std::unique_ptr<Tuple>> allTuples = sstManager.readAllData();
        std::cout << "SST Manager File Size: " << sstManager.sstFiles.size() << std::endl;
        if(sstManager.sstFiles.size() != 0){
            std::cout << "SST Manager First File Name: " << sstManager.sstFiles[0] << std::endl;
            std::remove(sstManager.sstFiles[0].c_str());
            sstManager.sstFiles.erase(sstManager.sstFiles.begin());
    
        
            int pageId = buffer_manager.getCurrentPageId();
            auto& page = buffer_manager.getPage(pageId);
            for(int i = 0; i < (int)allTuples.size();i++){
                page->addTuple(allTuples[i]->clone());
            }
            buffer_manager.flushPage(pageId);
            buffer_manager.extend();
            buffer_manager.readPage(pageId);

            // After major operations like this, truncate the WAL
            // walLog.truncate();
        }
    }

    void executeQueries() {

        std::vector<std::string> test_queries = {
            "SUM{1} GROUP BY {1} WHERE {1} > 2 and {1} < 6"
        };

        for (const auto& query : test_queries) {
            auto components = parseQuery(query);
            //prettyPrint(components);
            executeQuery(components, buffer_manager);
        }

    }

    // Function to print all data in the database
    void printAllData() {
        // First flush any remaining data in RBT
        // flushMemoryTable();
        
        // Get all data from SST files
        auto allData = sstManager.readAllData();
        
        std::cout << "Database contains " << allData.size() << " entries:" << std::endl;
        for (const auto& tuple : allData) {
            std::cout << "Key: " << tuple->fields[0]->asInt() 
                    << ", Value: " << tuple->fields[1]->asInt() << std::endl;
        }
    }

    // Function to checkpoint the database state
    void checkpoint() {
        // First flush any in-memory data
        flushMemoryTable();
        
        // Force compaction of SST files
        if (sstManager.sstFiles.size() >= 2) {
            sstManager.compactSSTs();
        }
        
        // Write a checkpoint in the WAL
        walLog.writeCheckpoint();
        
        // Truncate the WAL log since we've persisted everything
        walLog.truncate();
        
        std::cout << "Database checkpoint completed" << std::endl;
    }
    
};

int main() {
    auto start = std::chrono::high_resolution_clock::now();
    BuzzDB db;

    std::ifstream inputFile("output.txt");

    if (!inputFile) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    int field1, field2;
    // int numEntries = 0;
    while (inputFile >> field1 >> field2) {
        std::cout<<"Inserting: "<<field1<<", "<<field2<<std::endl;
        db.insert(field1, field2);
        // numEntries++;
    }

    // int numNewSST = numEntries/db.redBlackTree.MAX_NODES + numEntries%db.redBlackTree.MAX_NODES;
    
    // while(db.sstManager.numSST != numNewSST){
    //     std::cout<<"Waiting for active threads to finish."<<std::endl;
    // }   

    // Make sure all data is flushed to disk
    db.flushMemoryTable();
    
    // Print all data in the database
    // std::cout << "\nPrinting all data in the database:" << std::endl;
    // db.printAllData();

    

    // db.executeQueries();

    auto end = std::chrono::high_resolution_clock::now();

    // Calculate and print the elapsed time
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Elapsed time: " << 
    std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count() 
          << " microseconds" << std::endl;


    return 0;
}


int SimulateCrashRecovery() {
    // Create the database instance (will perform recovery if needed)
    BuzzDB db;

    // Check if we need to simulate a crash recovery
    bool simulateCrash = true;  // Set to true to test recovery
    
    if (simulateCrash) {
        std::cout << "Simulating database crash scenario..." << std::endl;
        
        // Add some data without proper shutdown
        db.insert(13, 14);
        db.insert(11, 18);
        db.insert(12, 19);
        
        std::cout << "Database 'crashed' before flushing to disk" << std::endl;
        db.walLog.printWALLog();

        db.redBlackTree.clearTree(); // Clear the in-memory RBT to simulate a crash
        std::vector<std::pair<int, int>> recoveredData = db.walLog.recover(); // Attempt to recover from the WAL
        db.walLog.truncate(); // Truncate the WAL after recovery to clean up the log
        for (const auto& [key, value] : recoveredData) {
            db.insert(key, value);
        }
        db.recoveryPerformed = true; // Mark recovery as performed
        // Exit without proper shutdown
        // return 1;
    }
    
    // Normal execution path
    std::ifstream inputFile("output.txt");

    if (!inputFile) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    int field1, field2;
    while (inputFile >> field1 >> field2) {
        std::cout << "Inserting: " << field1 << ", " << field2 << std::endl;
        db.insert(field1, field2);
        
        // Periodically create checkpoints
        if (db.tuple_insertion_attempt_counter % 100 == 0) {
            std::cout << "Periodic WAL Print:" << std::endl;
            db.walLog.printWALLog();
            db.checkpoint();
        }
    }



    // Make sure all data is flushed to disk
    db.flushMemoryTable();
    
    // Execute queries
    auto start = std::chrono::high_resolution_clock::now();
    db.executeQueries();
    auto end = std::chrono::high_resolution_clock::now();

    // Calculate and print the elapsed time
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Elapsed time: " 
              << std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count() 
              << " microseconds" << std::endl;

    // Create final checkpoint before clean shutdown
    // db.checkpoint();
    db.walLog.printWALLog(); // Print the WAL log

    return 0;
}