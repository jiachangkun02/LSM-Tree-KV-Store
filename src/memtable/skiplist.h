#pragma once
#include <vector>
#include <random>
#include <memory>
#include <cassert>

namespace lsmkv {

template <typename Key, typename Value, typename KeyComparator>
class SkipList {
public:
    explicit SkipList(int max_level = 16)
        : max_level_(max_level), level_(1), rnd_(0xdeadbeef) {
        head_ = new Node("", Value{}, max_level_);
        for (int i=0;i<max_level_;++i) head_->next[i] = nullptr;
    }

    ~SkipList() {
        Node* x = head_;
        while (x) { Node* n = x->next[0]; delete x; x = n; }
    }

    bool InsertOrAssign(const Key& key, const Value& value) {
        std::vector<Node*> update(max_level_, nullptr);
        Node* x = head_;
        for (int i = level_ - 1; i >=0; --i) {
            while (x->next[i] && comp_(x->next[i]->key, key) < 0) x = x->next[i];
            update[i] = x;
        }
        x = x->next[0];
        if (x && comp_(x->key, key) == 0) {
            x->value = value;
            return false;
        } else {
            int lvl = RandomLevel();
            if (lvl > level_) {
                for (int i = level_; i < lvl; ++i) update[i] = head_;
                level_ = lvl;
            }
            Node* n = new Node(key, value, lvl);
            for (int i=0;i<lvl;++i) {
                n->next[i] = update[i]->next[i];
                update[i]->next[i] = n;
            }
            return true;
        }
    }

    // Seek to first >= key
    class Iterator {
    public:
        explicit Iterator(Node* start) : node_(start) {}
        bool Valid() const { return node_ != nullptr; }
        const Key& key() const { return node_->key; }
        const Value& value() const { return node_->value; }
        void Next() { node_ = node_->next[0]; }
    private:
        Node* node_;
    };

    Iterator NewIterator() const { return Iterator(head_->next[0]); }

private:
    struct Node {
        Key key;
        Value value;
        std::vector<Node*> next;
        Node(const Key& k, const Value& v, int level) : key(k), value(v), next(level, nullptr) {}
    };

    int RandomLevel() {
        int lvl = 1;
        while ((rnd_() & 0xFFFF) < 0x8000 && lvl < max_level_) ++lvl;
        return lvl;
    }

    KeyComparator comp_;
    Node* head_;
    int max_level_;
    int level_;
    std::minstd_rand rnd_;
};

} // namespace lsmkv
