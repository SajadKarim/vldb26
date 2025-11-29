#pragma once
template <size_t N, typename... Types>
struct NthType;

template <typename FirstType, typename... RemainingTypes>
struct NthType<0, FirstType, RemainingTypes...> {
	using type = FirstType;
};

template <size_t N, typename FirstType, typename... RemainingTypes>
struct NthType<N, FirstType, RemainingTypes...> {
	using type = typename NthType<N - 1, RemainingTypes...>::type;
};

template <size_t N, typename... Args>
decltype(auto) getNthElement(Args&&... args) {
	static_assert(N < sizeof...(Args), "Index out of bounds");
	return std::get<N>(std::forward_as_tuple(std::forward<Args>(args)...));
}