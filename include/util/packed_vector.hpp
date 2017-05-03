#ifndef PACKED_VECTOR_HPP
#define PACKED_VECTOR_HPP

#include "util/integer_range.hpp"
#include "util/typedefs.hpp"
#include "util/vector_view.hpp"

#include "storage/io_fwd.hpp"
#include "storage/shared_memory_ownership.hpp"

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
inline T get_lower_half_value(WordT word, WordT mask, std::uint8_t offset, typename T::value_type * = 0)
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
inline T get_upper_half_value(WordT word, WordT mask, std::uint8_t offset, typename T::value_type * = 0)
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
    };

  public:
    using value_type = T;
    using block_type = WordT;

    class reference
    {
      public:
        reference(PackedVector &container, const InternalIndex internal_index)
            : container(container), internal_index(internal_index)
        {
        }

        reference &operator=(const value_type value) { container.set_value(internal_index, value); }

        operator T() const { return container.get_value(internal_index); }

        friend bool operator==(const value_type lhs, const reference &rhs)
        {
            return static_cast<T>(rhs) == lhs;
        }

        friend bool operator==(const reference &lhs, const value_type rhs)
        {
            return static_cast<T>(lhs) == rhs;
        }

        friend std::ostream &operator<<(std::ostream &lhs, const reference &rhs)
        {
            lhs << static_cast<T>(rhs);
            return lhs;
        }

      private:
        PackedVector &container;
        const InternalIndex internal_index;
    };

    PackedVector(std::initializer_list<T> list)
    {
        initialize_mask();
        reserve(list.size());
        for (const auto value : list)
            push_back(value);
    }

    PackedVector() { initialize_mask(); }

    PackedVector(util::ViewOrVector<std::uint64_t, Ownership> vec_, std::size_t num_elements)
        : vec(std::move(vec_)), num_elements(num_elements)
    {
        initialize_mask();
    }

    // forces the efficient read-only lookup
    auto peek(const std::size_t index) const { return operator[](index); }

    auto operator[](const std::size_t index) const { return get_value(get_internal_index(index)); }

    auto operator[](const std::size_t index) { return reference{*this, get_internal_index(index)}; }

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

    auto front() const { return operator[](0); }
    auto back() const { return operator[](num_elements - 1); }
    auto front() { return operator[](0); }
    auto back() { return operator[](num_elements - 1); }

    void push_back(const T value)
    {
        auto internal_index = get_internal_index(num_elements);

        while (internal_index.lower_word + 1 >= vec.size())
        {
            vec.push_back(0);
        }

        set_value(internal_index, value);
        num_elements++;

        BOOST_ASSERT(back() == value);
    }

    std::size_t size() const { return num_elements; }

    std::size_t size_blocks() const { return vec.size(); }

    std::size_t capacity() const { return (vec.capacity() / BLOCK_WORDS) * BLOCK_ELEMENTS; }

    template <bool enabled = (Ownership == storage::Ownership::View)>
    void reserve(typename std::enable_if<!enabled, std::size_t>::type capacity)
    {
        vec.reserve((capacity / BLOCK_ELEMENTS + 1) * BLOCK_WORDS);
    }

    friend void serialization::read<T, Bits, Ownership>(storage::io::FileReader &reader,
                                                        PackedVector &vec);

    friend void serialization::write<T, Bits, Ownership>(storage::io::FileWriter &writer,
                                                         const PackedVector &vec);

  private:
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
        const auto value =
            get_lower_half_value<WordT, T>(lower_word,
                                     lower_mask[internal_index.element],
                                     lower_offset[internal_index.element]) |
            get_upper_half_value<WordT, T>(upper_word, upper_mask[internal_index.element], upper_offset[internal_index.element]);
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
        upper_word =
            set_upper_value<WordT, T>(upper_word, upper_mask[internal_index.element], upper_offset[internal_index.element], value);
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
