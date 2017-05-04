#ifndef PACKED_VECTOR_HPP
#define PACKED_VECTOR_HPP

#include "util/integer_range.hpp"
#include "util/typedefs.hpp"
#include "util/vector_view.hpp"

#include "storage/io_fwd.hpp"
#include "storage/shared_memory_ownership.hpp"

#include <boost/iterator/iterator_facade.hpp>
#include <boost/iterator/reverse_iterator.hpp>

#include <cmath>
#include <vector>

namespace osrm
{
namespace util
{
namespace detail
{
template <typename T, std::size_t Bits, storage::Ownership Ownership> class PackedVector;
}

namespace serialization
{
template <typename T, std::size_t Bits, storage::Ownership Ownership>
inline void read(storage::io::FileReader &reader, detail::PackedVector<T, Bits, Ownership> &vec);

template <typename T, std::size_t Bits, storage::Ownership Ownership>
inline void write(storage::io::FileWriter &writer,
                  const detail::PackedVector<T, Bits, Ownership> &vec);
}

namespace detail
{

template <typename WordT, typename T>
inline T get_lower_half_value(WordT word,
                              WordT mask,
                              std::uint8_t offset,
                              typename std::enable_if_t<std::is_integral<T>::value> * = 0)
{
    return static_cast<T>((word & mask) >> offset);
}

template <typename WordT, typename T>
inline T
get_lower_half_value(WordT word, WordT mask, std::uint8_t offset, typename T::value_type * = 0)
{
    return T{static_cast<typename T::value_type>((word & mask) >> offset)};
}

template <typename WordT, typename T>
inline T get_upper_half_value(WordT word,
                              WordT mask,
                              std::uint8_t offset,
                              typename std::enable_if_t<std::is_integral<T>::value> * = 0)
{
    return static_cast<T>((word & mask) << offset);
}

template <typename WordT, typename T>
inline T
get_upper_half_value(WordT word, WordT mask, std::uint8_t offset, typename T::value_type * = 0)
{
    return T{static_cast<typename T::value_type>((word & mask) << offset)};
}

template <typename WordT, typename T>
inline WordT set_lower_value(WordT word, WordT mask, std::uint8_t offset, T value)
{
    return (word & ~mask) | (static_cast<WordT>(value) << offset);
}

template <typename WordT, typename T>
inline WordT set_upper_value(WordT word, WordT mask, std::uint8_t offset, T value)
{
    return (word & ~mask) | (static_cast<WordT>(value) >> offset);
}

template <typename T, std::size_t Bits, storage::Ownership Ownership> class PackedVector
{
    using WordT = std::uint64_t;

    // This fails for all strong typedef types
    // static_assert(std::is_integral<T>::value, "T must be an integral type.");
    static_assert(sizeof(T) <= sizeof(WordT), "Maximum size of type T is 8 bytes");
    static_assert(Bits > 0, "Minimum number of bits is 0.");
    static_assert(Bits <= sizeof(WordT) * CHAR_BIT, "Maximum number of bits is 64.");

    static constexpr std::size_t WORD_BITS = sizeof(WordT) * CHAR_BIT;
    // number of elements per block, use the number of bits so we make sure
    // we can devide the total number of bits by the element bis
  public:
    static constexpr std::size_t BLOCK_ELEMENTS = WORD_BITS;

  private:
    // number of words per block
    static constexpr std::size_t BLOCK_WORDS = (Bits * BLOCK_ELEMENTS) / WORD_BITS;

    // mask for the lower/upper word of a record
    std::array<WordT, BLOCK_ELEMENTS> lower_mask;
    std::array<WordT, BLOCK_ELEMENTS> upper_mask;
    std::array<std::uint8_t, BLOCK_ELEMENTS> lower_offset;
    std::array<std::uint8_t, BLOCK_ELEMENTS> upper_offset;
    // in which word of the block is the element
    std::array<std::uint8_t, BLOCK_ELEMENTS> word_offset;

    struct InternalIndex
    {
        // index to the word that contains the lower
        // part of the value
        // note: upper_word == lower_word + 1
        std::size_t lower_word;
        // index to the element of the block
        std::uint8_t element;

        bool operator==(const InternalIndex &other) const { return std::tie(lower_word, element) == std::tie(other.lower_word, other.element); }
    };

  public:
    using value_type = T;
    using block_type = WordT;

    class internal_reference
    {
      public:
        internal_reference(PackedVector &container, const InternalIndex internal_index)
            : container(container), internal_index(internal_index)
        {
        }

        internal_reference &operator=(const value_type value)
        {
            container.set_value(internal_index, value);
            return *this;
        }

        operator T() const { return container.get_value(internal_index); }

        bool operator==(const internal_reference &other) const
        {
            return &container == &other.container && internal_index == other.internal_index;
        }

        // I would remove this and use explicit casting if needed
        // friend bool operator==(const internal_reference &lhs, const value_type rhs)
        // {
        //     return static_cast<T>(lhs) == rhs;
        // }

        // friend bool operator==(const value_type lhs, const internal_reference &rhs)
        // {
        //     return lhs == static_cast<T>(rhs);
        // }

        friend std::ostream &operator<<(std::ostream &os, const internal_reference &rhs)
        {
            return os << static_cast<T>(rhs);
        }

      private:
        PackedVector &container;
        const InternalIndex internal_index;
    };

    template<typename DataT>
    class iterator_impl
        : public boost::iterator_facade<iterator_impl<DataT>, DataT, boost::random_access_traversal_tag, internal_reference>
    {
        typedef boost::iterator_facade<iterator_impl<DataT>, DataT, boost::random_access_traversal_tag, internal_reference> base_t;

      public:
        typedef typename base_t::value_type value_type;
        typedef typename base_t::difference_type difference_type;
        typedef typename base_t::reference reference;
        typedef std::random_access_iterator_tag iterator_category;

        explicit iterator_impl() : container(nullptr), index(std::numeric_limits<std::size_t>::max()) {}
        explicit iterator_impl(PackedVector *container, const std::size_t index)
            : container(container), index(index)
        {
        }

      private:
        void increment() { ++index; }
        void decrement() { --index; }
        void advance(difference_type offset) { index += offset; }
        bool equal(const iterator_impl &other) const { return container->peek(index) == other.container->peek(other.index); }
        reference dereference() const { return (*container)[index]; }
        difference_type distance_to(const iterator_impl &other) const
        {
            return other.index - index;
        }

      private:
        PackedVector *container;
        std::size_t index;

        friend class ::boost::iterator_core_access;
    };

    using iterator = iterator_impl<T>;
    using const_iterator = iterator_impl<const T>;
    using reverse_iterator = boost::reverse_iterator<iterator>;

    PackedVector(std::initializer_list<T> list)
    {
        initialize_mask();
        reserve(list.size());
        for (const auto value : list)
            push_back(value);
    }

    PackedVector() { initialize_mask(); }

    PackedVector(std::size_t size)
    {
        initialize_mask();
        resize(size);
    }

    PackedVector(util::ViewOrVector<std::uint64_t, Ownership> vec_, std::size_t num_elements)
        : vec(std::move(vec_)), num_elements(num_elements)
    {
        initialize_mask();
    }

    // forces the efficient read-only lookup
    auto peek(const std::size_t index) const { return operator[](index); }

    auto operator[](const std::size_t index) const { return get_value(get_internal_index(index)); }

    auto operator[](const std::size_t index) { return internal_reference{*this, get_internal_index(index)}; }

    auto at(std::size_t index) const
    {
        if (index < num_elements)
            return operator[](index);
        else
            throw std::out_of_range(std::to_string(index) + " is bigger then container size " +
                                    std::to_string(num_elements));
    }

    auto at(std::size_t index)
    {
        if (index < num_elements)
            return operator[](index);
        else
            throw std::out_of_range(std::to_string(index) + " is bigger then container size " +
                                    std::to_string(num_elements));
    }

    auto begin() { return iterator(this, 0); }

    auto end() { return iterator(this, num_elements); }

    auto cbegin() const { return const_iterator(this, 0); }

    auto cend() const { return const_iterator(this, num_elements); }

    auto rbegin() const { return reverse_iterator(end()); }

    auto rend() const { return reverse_iterator(begin()); }

    auto front() const { return operator[](0); }
    auto back() const { return operator[](num_elements - 1); }
    auto front() { return operator[](0); }
    auto back() { return operator[](num_elements - 1); }

    void push_back(const T value)
    {
        auto internal_index = get_internal_index(num_elements);

        while (internal_index.lower_word + 1 >= vec.size())
        {
            allocate_blocks(1);
        }

        set_value(internal_index, value);
        num_elements++;

        BOOST_ASSERT(static_cast<T>(back()) == value);
    }

    std::size_t size() const { return num_elements; }

    void resize(std::size_t elements)
    {
        num_elements = elements;
        auto num_blocks = std::ceil(static_cast<double>(elements) / BLOCK_ELEMENTS);
        vec.resize(num_blocks * BLOCK_WORDS);
    }

    std::size_t capacity() const { return (vec.capacity() / BLOCK_WORDS) * BLOCK_ELEMENTS; }

    template <bool enabled = (Ownership == storage::Ownership::View)>
    void reserve(typename std::enable_if<!enabled, std::size_t>::type capacity)
    {
        auto num_blocks = std::ceil(static_cast<double>(capacity) / BLOCK_ELEMENTS);
        vec.reserve(num_blocks * BLOCK_WORDS);
    }

    friend void serialization::read<T, Bits, Ownership>(storage::io::FileReader &reader,
                                                        PackedVector &vec);

    friend void serialization::write<T, Bits, Ownership>(storage::io::FileWriter &writer,
                                                         const PackedVector &vec);

  private:
    void allocate_blocks(std::size_t num_blocks)
    {
        vec.resize(vec.size() + num_blocks * BLOCK_WORDS);
    }

    inline InternalIndex get_internal_index(const std::size_t index) const
    {
        const auto block_offset = BLOCK_ELEMENTS * (index / BLOCK_ELEMENTS);
        const std::uint8_t element_index = index % BLOCK_ELEMENTS;
        const auto lower_word_index = block_offset + word_offset[element_index];

        return InternalIndex{lower_word_index, element_index};
    }

    inline T get_value(const InternalIndex internal_index) const
    {
        const auto lower_word = vec[internal_index.lower_word];
        // note this can actually already be a word of the next block however in
        // that case the upper mask will be 0.
        // we make sure to have a sentinel element to avoid out-of-bounds errors.
        const auto upper_word = vec[internal_index.lower_word + 1];
        const auto value = get_lower_half_value<WordT, T>(lower_word,
                                                          lower_mask[internal_index.element],
                                                          lower_offset[internal_index.element]) |
                           get_upper_half_value<WordT, T>(upper_word,
                                                          upper_mask[internal_index.element],
                                                          upper_offset[internal_index.element]);
        return value;
    }

    inline void set_value(const InternalIndex internal_index, const T value)
    {
        auto &lower_word = vec[internal_index.lower_word];
        auto &upper_word = vec[internal_index.lower_word + 1];
        lower_word = set_lower_value<WordT, T>(lower_word,
                                               lower_mask[internal_index.element],
                                               lower_offset[internal_index.element],
                                               value);
        upper_word = set_upper_value<WordT, T>(upper_word,
                                               upper_mask[internal_index.element],
                                               upper_offset[internal_index.element],
                                               value);
    }

    void initialize_mask()
    {
        // TODO port to constexptr function
        const std::uint64_t mask = (1UL << Bits) - 1;
        auto offset = 0;
        for (auto element_index : util::irange<std::uint8_t>(0, BLOCK_ELEMENTS))
        {
            auto local_offset = offset % 64;
            lower_mask[element_index] = mask << local_offset;
            lower_offset[element_index] = local_offset;
            // check we sliced off bits
            if (local_offset + Bits > WORD_BITS)
            {
                upper_mask[element_index] = mask >> (WORD_BITS - local_offset);
                upper_offset[element_index] = WORD_BITS - local_offset;
            }
            else
            {
                upper_mask[element_index] = 0;
                upper_offset[element_index] = Bits;
            }
            word_offset[element_index] = offset / WORD_BITS;
            offset += Bits;
        }
    }

    util::ViewOrVector<std::uint64_t, Ownership> vec;
    std::uint64_t num_elements = 0;
};
}

template <typename T, std::size_t Bits>
using PackedVector = detail::PackedVector<T, Bits, storage::Ownership::Container>;
template <typename T, std::size_t Bits>
using PackedVectorView = detail::PackedVector<T, Bits, storage::Ownership::View>;
}
}

#endif /* PACKED_VECTOR_HPP */
