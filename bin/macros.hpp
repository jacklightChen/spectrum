#define DEFER99(...) DEFER98(__VA_ARGS__)
#define DEFER98(...) DEFER97(__VA_ARGS__)
#define DEFER97(...) DEFER96(__VA_ARGS__)
#define DEFER96(...) DEFER95(__VA_ARGS__)
#define DEFER95(...) DEFER94(__VA_ARGS__)
#define DEFER94(...) DEFER93(__VA_ARGS__)
#define DEFER93(...) DEFER92(__VA_ARGS__)
#define DEFER92(...) DEFER91(__VA_ARGS__)
#define DEFER91(...) DEFER90(__VA_ARGS__)
#define DEFER90(...) DEFER89(__VA_ARGS__)
#define DEFER89(...) DEFER88(__VA_ARGS__)
#define DEFER88(...) DEFER87(__VA_ARGS__)
#define DEFER87(...) DEFER86(__VA_ARGS__)
#define DEFER86(...) DEFER85(__VA_ARGS__)
#define DEFER85(...) DEFER84(__VA_ARGS__)
#define DEFER84(...) DEFER83(__VA_ARGS__)
#define DEFER83(...) DEFER82(__VA_ARGS__)
#define DEFER82(...) DEFER81(__VA_ARGS__)
#define DEFER81(...) DEFER80(__VA_ARGS__)
#define DEFER80(...) DEFER79(__VA_ARGS__)
#define DEFER79(...) DEFER78(__VA_ARGS__)
#define DEFER78(...) DEFER77(__VA_ARGS__)
#define DEFER77(...) DEFER76(__VA_ARGS__)
#define DEFER76(...) DEFER75(__VA_ARGS__)
#define DEFER75(...) DEFER74(__VA_ARGS__)
#define DEFER74(...) DEFER73(__VA_ARGS__)
#define DEFER73(...) DEFER72(__VA_ARGS__)
#define DEFER72(...) DEFER71(__VA_ARGS__)
#define DEFER71(...) DEFER70(__VA_ARGS__)
#define DEFER70(...) DEFER69(__VA_ARGS__)
#define DEFER69(...) DEFER68(__VA_ARGS__)
#define DEFER68(...) DEFER67(__VA_ARGS__)
#define DEFER67(...) DEFER66(__VA_ARGS__)
#define DEFER66(...) DEFER65(__VA_ARGS__)
#define DEFER65(...) DEFER64(__VA_ARGS__)
#define DEFER64(...) DEFER63(__VA_ARGS__)
#define DEFER63(...) DEFER62(__VA_ARGS__)
#define DEFER62(...) DEFER61(__VA_ARGS__)
#define DEFER61(...) DEFER60(__VA_ARGS__)
#define DEFER60(...) DEFER59(__VA_ARGS__)
#define DEFER59(...) DEFER58(__VA_ARGS__)
#define DEFER58(...) DEFER57(__VA_ARGS__)
#define DEFER57(...) DEFER56(__VA_ARGS__)
#define DEFER56(...) DEFER55(__VA_ARGS__)
#define DEFER55(...) DEFER54(__VA_ARGS__)
#define DEFER54(...) DEFER53(__VA_ARGS__)
#define DEFER53(...) DEFER52(__VA_ARGS__)
#define DEFER52(...) DEFER51(__VA_ARGS__)
#define DEFER51(...) DEFER50(__VA_ARGS__)
#define DEFER50(...) DEFER49(__VA_ARGS__)
#define DEFER49(...) DEFER48(__VA_ARGS__)
#define DEFER48(...) DEFER47(__VA_ARGS__)
#define DEFER47(...) DEFER46(__VA_ARGS__)
#define DEFER46(...) DEFER45(__VA_ARGS__)
#define DEFER45(...) DEFER44(__VA_ARGS__)
#define DEFER44(...) DEFER43(__VA_ARGS__)
#define DEFER43(...) DEFER42(__VA_ARGS__)
#define DEFER42(...) DEFER41(__VA_ARGS__)
#define DEFER41(...) DEFER40(__VA_ARGS__)
#define DEFER40(...) DEFER39(__VA_ARGS__)
#define DEFER39(...) DEFER38(__VA_ARGS__)
#define DEFER38(...) DEFER37(__VA_ARGS__)
#define DEFER37(...) DEFER36(__VA_ARGS__)
#define DEFER36(...) DEFER35(__VA_ARGS__)
#define DEFER35(...) DEFER34(__VA_ARGS__)
#define DEFER34(...) DEFER33(__VA_ARGS__)
#define DEFER33(...) DEFER32(__VA_ARGS__)
#define DEFER32(...) DEFER31(__VA_ARGS__)
#define DEFER31(...) DEFER30(__VA_ARGS__)
#define DEFER30(...) DEFER29(__VA_ARGS__)
#define DEFER29(...) DEFER28(__VA_ARGS__)
#define DEFER28(...) DEFER27(__VA_ARGS__)
#define DEFER27(...) DEFER26(__VA_ARGS__)
#define DEFER26(...) DEFER25(__VA_ARGS__)
#define DEFER25(...) DEFER24(__VA_ARGS__)
#define DEFER24(...) DEFER23(__VA_ARGS__)
#define DEFER23(...) DEFER22(__VA_ARGS__)
#define DEFER22(...) DEFER21(__VA_ARGS__)
#define DEFER21(...) DEFER20(__VA_ARGS__)
#define DEFER20(...) DEFER19(__VA_ARGS__)
#define DEFER19(...) DEFER18(__VA_ARGS__)
#define DEFER18(...) DEFER17(__VA_ARGS__)
#define DEFER17(...) DEFER16(__VA_ARGS__)
#define DEFER16(...) DEFER15(__VA_ARGS__)
#define DEFER15(...) DEFER14(__VA_ARGS__)
#define DEFER14(...) DEFER13(__VA_ARGS__)
#define DEFER13(...) DEFER12(__VA_ARGS__)
#define DEFER12(...) DEFER11(__VA_ARGS__)
#define DEFER11(...) DEFER10(__VA_ARGS__)
#define DEFER10(...) DEFER09(__VA_ARGS__)
#define DEFER09(...) DEFER08(__VA_ARGS__)
#define DEFER08(...) DEFER07(__VA_ARGS__)
#define DEFER07(...) DEFER06(__VA_ARGS__)
#define DEFER06(...) DEFER05(__VA_ARGS__)
#define DEFER05(...) DEFER04(__VA_ARGS__)
#define DEFER04(...) DEFER03(__VA_ARGS__)
#define DEFER03(...) DEFER02(__VA_ARGS__)
#define DEFER02(...) DEFER01(__VA_ARGS__)
#define DEFER01(...) DEFER00(__VA_ARGS__)
#define DEFER00(...) __VA_ARGS__
#define PARENS ()
#define FOR_EACH(macro, ...)            __VA_OPT__(DEFER99(FOR_EACH_HELPER(macro, __VA_ARGS__)))
#define FOR_EACH_HELPER(macro, a1, ...) macro(a1, __VA_ARGS__) __VA_OPT__(FOR_EACH_AGAIN PARENS (macro, __VA_ARGS__))
#define FOR_EACH_AGAIN() FOR_EACH_HELPER
#define COUNT_HELPER(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21, _22, _23, _24, _25, _26, _27, _28, _29, _30, _31, _32, _33, _34, _35, _36, _37, _38, _39, _40, _41, _42, _43, _44, _45, _46, _47, _48, _49, _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, _60, _61, _62, _63, _64, _65, _66, _67, _68, _69, _70, _71, _72, _73, _74, _75, _76, _77, _78, _79, _80, _81, _82, _83, _84, _85, _86, _87, _88, _89, _90, _91, _92, _93, _94, _95, _96, _97, _98, _99, N, ...) N
#define COUNT(...) COUNT_HELPER(__VA_ARGS__, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define NAME(...)  COUNT_HELPER(__VA_ARGS__, __99, __98, __97, __96, __95, __94, __93, __92, __91, __90, __89, __88, __87, __86, __85, __84, __83, __82, __81, __80, __79, __78, __77, __76, __75, __74, __73, __72, __71, __70, __69, __68, __67, __66, __65, __64, __63, __62, __61, __60, __59, __58, __57, __56, __55, __54, __53, __52, __51, __50, __49, __48, __47, __46, __45, __44, __43, __42, __41, __40, __39, __38, __37, __36, __35, __34, __33, __32, __31, __30, __29, __28, __27, __26, __25, __24, __23, __22, __21, __20, __19, __18, __17, __16, __15, __14, __13, __12, __11, __10, __9, __8, __7, __6, __5, __4, __3, __2, __1, __0)

// test macros
static_assert(COUNT(a,b,c) == 3);
static_assert(COUNT(a,b,c,d) == 4);
#define TEST_FOR_EACH(X, ...) X*5 __VA_OPT__(,)
static_assert(std::array<int, 4>{FOR_EACH(TEST_FOR_EACH, 1, 2, 3, 4)} == std::array<int, 4>{5, 10, 15, 20});
#undef TEST_FOR_EACH