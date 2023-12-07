require(['jquery', 'bootstrap'], function ($) {

    const MAIN_SEARCH_PATH = "Main/Search";
    const VIEW_PAGE_PATH = "/bin/view";
    const SEARCH_PARAM = "?text=";
    const SOURCE_URI_SCOPE_PREFIX = "_source_uri:";
    const SEARCH_FULL_URL = window.location.href;
    const SEARCH_PATH_QUERY_PARAM_KEY = "searchPath";

    const COMBO_KEYWORD_DISALLOW_OP_ERROR = document.getElementById('keyword_invalid_error').value;
    const KEYWORD_DISALLOW_OP_ERROR = document.getElementById('disallow_operator_error').value;
    const ALIAS_KEYWORD_DISALLOW_SPACES_ERROR = document.getElementById('alias_invalid_space_error').value;
    const ALIAS_KEYWORD_DISALLOW_NONLETTER_ERROR = document.getElementById('alias_invalid_char_error').value;
    const WILDCARD_KEYWORD_DISALLOW_LESS_CHAR_ERROR = document.getElementById('wildcard_invalid_format_error').value;
    const WILDCARD_KEYWORD_NO_STAR_ERROR = document.getElementById('wildcard_no_star_error').value;
    const KEYS_COMBO_GUIDE_TEXT = document.getElementById('keyword_terms').value;
    const WILDCARD_GUIDE_TEXT = document.getElementById('wildcard_terms').value;
    const ALIAS_GUIDE_TEXT = document.getElementById('alias_terms').value;
    const TAGS_GUIDE_TEXT = document.getElementById('tags_terms').value;

    const advancedsearch_elements = ["combo_keyword_0", "combo_keyword_1", "combo_keyword_2", "wildcard_keyword", "creator_keyword", "tag_keyword"];
    const tag_label = "tag";
    const wildcard_label = "wildcard";
    const alias_label = "creator-alias";
    const combo_key0_label = "combo-key-word-0";
    const combo_key1_label = "combo-key-word-1";
    const combo_key2_label = "combo-key-word-2";

    window.addEventListener('load', function () {
        /* Adjust search query when a tree search is performed from the search bar */
        const currentUrl = new URL(document.location);
        const searchParams = currentUrl.searchParams;
        const searchPath = searchParams.get(SEARCH_PATH_QUERY_PARAM_KEY);
        let searchText = $("#headerglobalsearchinput").val();
        if (searchPath != null && searchPath != "" && !searchText.includes(SOURCE_URI_SCOPE_PREFIX)) {
            if (searchText == "") {
                searchText = `${SOURCE_URI_SCOPE_PREFIX}"${searchPath}"`;
                $("#headerglobalsearchinput").val(searchText);
            } else {
                searchText = `${searchText} AND ${SOURCE_URI_SCOPE_PREFIX}"${searchPath}"`;
                $("#headerglobalsearchinput").val(searchText);
            }
            $("#document-title").find("h1").html(`Search: ${searchText}`);
        }
    });

    /**
     * Check if combo keyword is valid or not.
     * 1. Boolean operators: AND, OR, NOT are not allowed
     * 2. * is not allowed
     * @param content keyword in each Keyword combination input box
     * @returns {string|null} keyword combination input error message
     */
    function comboKeyWordCheck(content) {
        if (content == null || content.length === 0) {
            return null;
        }

        let input_keyword = content.trim();
        if(checkIfIncludeDisallowedOperator(content) || input_keyword.includes('*')) {
            return COMBO_KEYWORD_DISALLOW_OP_ERROR;
        }

        return null;
    }

    /**
     * Check if wildcard keyword is valid or not.
     * 1. * must be included
     * 2. Must be at least 2 characters before *
     * 3. Boolean operators: AND, OR, NOT are not allowed
     * @param content keyword in Wildcard input box
     * @returns {string|null} Wildcard input error message
     */
    function wildcardKeyWordCheck(content) {
        if (content == null || content.length === 0) {
            return null;
        }

        let wildcard_input = content.trim();
        if (wildcard_input && wildcard_input.includes('*')) {
            // Must be at least 2 characters before *
            const ast_index = wildcard_input.indexOf('*');
            if (ast_index < 2) {
                return WILDCARD_KEYWORD_DISALLOW_LESS_CHAR_ERROR;
            }
        } else {
            return WILDCARD_KEYWORD_NO_STAR_ERROR;
        }

        if(checkIfIncludeDisallowedOperator(content)) {
            return KEYWORD_DISALLOW_OP_ERROR;
        }
        return null;
    }

    /**
     * Check if creator alias is valid or not.
     * 1. Space(s) is not allowed
     * 2. Only allows [a-zA-Z]
     * @param content keyword in User alias input box
     * @returns {string|null} User alias input error message
     */
    function creatorAliasCheck(content) {
        if (content == null || content.length === 0) {
            return null;
        }

        let alias_input = content.trim();
        if(/\s/.test(alias_input)) {
            return ALIAS_KEYWORD_DISALLOW_SPACES_ERROR;
        }

        if (!/^[a-zA-Z0-9.-]+$/.test(alias_input)) {
            return ALIAS_KEYWORD_DISALLOW_NONLETTER_ERROR;
        }

        return null;
    }

    /**
     * Check if tag input is valid or not.
     * 1. Boolean operators: AND, OR, NOT are not allowed
     * @param content keyword in Tags input box
     * @returns {string|null} Tags input error message
     */
    function tagCheck(content) {
        if (content == null || content.length === 0) {
            return null;
        }

        if(checkIfIncludeDisallowedOperator(content)) {
            return KEYWORD_DISALLOW_OP_ERROR;
        }
        return null;
    }

    function checkIfIncludeDisallowedOperator(content) {
        let input_keyword = content.trim();
        let input_words = input_keyword.split(/\s+/);
        for(const word of input_words) {
            if(word === "OR" || word === "AND" || word === "NOT") {
                return true;
            }
        }
        return false;
    }

    function retainSourceUri() {
        const query = decodeURIComponent(SEARCH_FULL_URL);
        if (query.includes(SOURCE_URI_SCOPE_PREFIX)) {
            $("#search-tree-toggle").prop("checked", true);
            $("#search-tree-icon").toggleClass("search-tree-icon-inactive search-tree-icon-active");
            $("#search-tree-icon-outer-div").toggleClass("search-tree-icon-outer-div-inactive search-tree-icon-outer-div-active");
            return;
        }

        const currentUrl = new URL(document.location);
        const searchParams = currentUrl.searchParams;
        const searchPath = searchParams.get(SEARCH_PATH_QUERY_PARAM_KEY);
        if (searchPath != null) {
            if (searchPath != "") {
                $("#search-tree-toggle").prop("checked", true);
                $("#search-tree-icon").toggleClass("search-tree-icon-inactive search-tree-icon-active");
                $("#search-tree-icon-outer-div").toggleClass("search-tree-icon-outer-div-inactive search-tree-icon-outer-div-active");
            }
        }
    }

    /**
     * A handler to validate user input and return corresponding error message.
     * @param label
     * @param validationMethod
     * @param text_guide
     */
    function inputValidationAndInitializationHandler(label, validationMethod, text_guide) {
        let form_group_id = "#form-group-" + label;
        let form_group_help_block_id = "#form-group-" + label + "-help-block";

        if(!validationMethod) {
            $(form_group_id).removeClass("has-error");
            $(form_group_help_block_id).text(text_guide);
        } else {
            const error_info = validationMethod(event.target.value);
            if (error_info != null) {
                $(form_group_id).addClass("has-error");
                $(form_group_help_block_id).text(error_info);
            } else {
                $(form_group_id).removeClass("has-error");
                $(form_group_help_block_id).text(text_guide);
            }
        }
    }

    /* Retain search-within-a-tree selection */
    retainSourceUri();

    /**
     * Fired while user is typing in tag_keyword
     * */
    $("#tag_keyword").on("keyup", function(event) {
        inputValidationAndInitializationHandler(tag_label, tagCheck, TAGS_GUIDE_TEXT);
    });

    /**
     * Fired while user is typing in wildcard_keyword
     * */
    $("#wildcard_keyword").on("keyup", function(event) {
        inputValidationAndInitializationHandler(wildcard_label, wildcardKeyWordCheck, WILDCARD_GUIDE_TEXT);
    });

    /**
     * Fired while user is typing in creator_keyword
     * */
    $("#creator_keyword").on("keyup", function(event) {
        inputValidationAndInitializationHandler(alias_label, creatorAliasCheck, ALIAS_GUIDE_TEXT);
    });

    /**
     * Fired when while user is typing in combo_keyword_0
     * */
    $("#combo_keyword_0").on("keyup", function(event) {
        inputValidationAndInitializationHandler(combo_key0_label, comboKeyWordCheck, KEYS_COMBO_GUIDE_TEXT);
    });

    /**
     * Fired when while user is typing in combo_keyword_1
     * */
    $("#combo_keyword_1").on("keyup", function(event) {
        inputValidationAndInitializationHandler(combo_key1_label, comboKeyWordCheck, KEYS_COMBO_GUIDE_TEXT);
    });

    /**
     * Fired when while user is typing in combo_keyword_2
     * */
    $("#combo_keyword_2").on("keyup", function(event) {
        inputValidationAndInitializationHandler(combo_key2_label, comboKeyWordCheck, KEYS_COMBO_GUIDE_TEXT);
    });

    /**
     * [Advanced Search Icon Click handle] Parse URL and populate data in each input field in Advanced Search modal
     */
    $("#advanced-search-icon").on("click", function() {
        const currentUrl = new URL(document.location);
        if(currentUrl && !currentUrl.toString().includes("&isadvancedsearch=1")) {
            return;
        }
        const CLEANED_SEARCH_QUERY = currentUrl.searchParams.get("text");
        if (CLEANED_SEARCH_QUERY != null && CLEANED_SEARCH_QUERY != "") {
            let tagFilterExist = false;
            let creatorFilterExist = false;
            if(CLEANED_SEARCH_QUERY.includes("category_list:")) {
                tagFilterExist = true;
            }
            if(CLEANED_SEARCH_QUERY.includes("creator:") && CLEANED_SEARCH_QUERY.includes("last_modifier:")) {
                creatorFilterExist = true;
            }
            let REMOVED_SOURCE_URI = parseSourceUriAndCleanUp(CLEANED_SEARCH_QUERY);
            let REMOVE_TAGS_URL = parseTagsInputAndCleanUp(REMOVED_SOURCE_URI);
            let REMOVED_CREATOR_URL = parseCreatorAliasAndCleanUp(REMOVE_TAGS_URL);
            parseKeysCombination(REMOVED_CREATOR_URL, creatorFilterExist, tagFilterExist);
        }
    });

    // Retrieve Tag field input from search query & clean up Tag param from URL
    function parseTagsInputAndCleanUp(CLEANED_SEARCH_QUERY) {
        let REMOVE_TAGS_URL = CLEANED_SEARCH_QUERY;
        if(CLEANED_SEARCH_QUERY && CLEANED_SEARCH_QUERY.includes("category_list")) {
            // Retrieve TAGS input from URL
            let TAG_INPUT = CLEANED_SEARCH_QUERY.match(/category_list.*/);
            let TAGS = "";
            if(TAG_INPUT && TAG_INPUT.length > 0) {
                let TAG_INPUT_VAL = TAG_INPUT[0];
                let cleaned_tags = TAG_INPUT_VAL.replace("AND", "").trim();
                TAGS = cleaned_tags.replace("category_list:", "");
            }
            document.getElementById("tag_keyword").value = TAGS;

            // Remove category_list param and value after it
            REMOVE_TAGS_URL = CLEANED_SEARCH_QUERY.replace(/category_list.*/, "");
        }
        return REMOVE_TAGS_URL;
    }

    // Retrieve Creator/Last modifier alias from search query & clean up Creator/Last modifier param & value from query
    function parseCreatorAliasAndCleanUp(REMOVE_TAGS_URL) {
        let REMOVED_CREATOR_URL = REMOVE_TAGS_URL;
        if(REMOVE_TAGS_URL && REMOVE_TAGS_URL.includes("creator")) {
            // Retrieve alias input
            let CREATOR_INPUT = REMOVE_TAGS_URL.match(/creator.*/);
            if(CREATOR_INPUT && CREATOR_INPUT.length > 0) {
                let CREATOR_INPUT_VAL = CREATOR_INPUT[0];
                // Example: creator:"alias" OR last_modifier:"alias"
                if(CREATOR_INPUT_VAL && CREATOR_INPUT_VAL.includes("OR")) {
                    let creator = CREATOR_INPUT_VAL.split('OR')[0].trim();
                    let alias = creator.substring(9, creator.length - 1);
                    document.getElementById("creator_keyword").value = alias;
                }
            }

            // Remove creator and the val after it
            REMOVED_CREATOR_URL = REMOVE_TAGS_URL.replace(/creator.*/, "");
        }
        return REMOVED_CREATOR_URL;
    }

    function parseSourceUriAndCleanUp(REMOVED_CREATOR_URL) {
        let REMOVED_SOURCE_URI = REMOVED_CREATOR_URL;
        let terms = REMOVED_CREATOR_URL.split(" AND ");
        const sourceUriIndex = terms.findIndex(term => term.includes(SOURCE_URI_SCOPE_PREFIX) || term.includes(SEARCH_PATH_QUERY_PARAM_KEY));
        if (sourceUriIndex != -1) {
            terms.splice(sourceUriIndex);
        }
        REMOVED_SOURCE_URI = terms.join(" AND ");
        
        return REMOVED_SOURCE_URI;
    }

    function parseWildcardInputAndCleanUp(cleaned_keys_input) {
        let REMOVED_WILDCARD_URL = cleaned_keys_input;
        // parse wildcard input if any
        if(cleaned_keys_input && cleaned_keys_input.includes("*")) {
            if(cleaned_keys_input.includes("AND")) {
                // * is only allowed in wildcard input
                let keys_input = cleaned_keys_input.split("AND");
                if(keys_input && keys_input.length > 1) {
                    let wildcard_input = keys_input[keys_input.length - 1].trim();
                    document.getElementById("wildcard_keyword").value = wildcard_input;
                    REMOVED_WILDCARD_URL = cleaned_keys_input.replace(wildcard_input, "").trim();
                }
            } else {
                document.getElementById("wildcard_keyword").value = cleaned_keys_input;
                return "";
            }
        }
        return REMOVED_WILDCARD_URL;
    }

    // Retrieve keys/wildcard values from search query & set input value based on them
    function parseKeysCombination(REMOVED_CREATOR_URL, creatorFilterExist, tagFilterExist) {
        if(!REMOVED_CREATOR_URL) {
            return;
        }

        let cleaned_keys = REMOVED_CREATOR_URL;
        if(tagFilterExist || creatorFilterExist) {
            if(creatorFilterExist) {
                // Remove postfix if creator filter exist. Example:
                // ( test OR amazon ) AND people ama* AND (creator:"X" OR last_modifier:"X") AND category_list:wiki
                cleaned_keys = REMOVED_CREATOR_URL.replace("AND (", "").trim();
            } else {
                // Remove postfix if creator filter NOT exist. Example:
                // ( test OR amazon ) AND people ama* AND category_list:wiki
                if(REMOVED_CREATOR_URL && REMOVED_CREATOR_URL.length >= 4) {
                    cleaned_keys = REMOVED_CREATOR_URL.substring(0, REMOVED_CREATOR_URL.length - 4).trim();
                }
            }
        }

        let removed_wildcard_query = parseWildcardInputAndCleanUp(cleaned_keys);
        if(removed_wildcard_query) {
            let keys_input_arr = removed_wildcard_query.split(/\s+/);
            let keywords = [];
            if(cleaned_keys.includes("AND") || cleaned_keys.includes("OR") || cleaned_keys.includes("NOT")) {
                //1. find operators
                keys_input_arr.forEach(function (item, index) {
                    let cleaned_key = item.trim();
                    if(cleaned_key != "(" && cleaned_key != ")") {
                        keywords.push(cleaned_key);
                    }
                });
                let first_operator_exist = false
                let second_operator_exist = false
                let first_key = "";
                let second_key = "";
                let third_key = "";
                for(const key of keywords) {
                    if(key == "AND" || key == "OR" || key == "NOT") {
                        if(!first_operator_exist) {
                            document.getElementById("combo_op_0").value = key;
                            first_operator_exist = true;
                        } else if(!second_operator_exist) {
                            document.getElementById("combo_op_1").value = key;
                            second_operator_exist = true;
                        }
                    } else {
                        if(!first_operator_exist) {
                            first_key = (!first_key)? key : first_key + " " + key;
                        } else if(!second_operator_exist) {
                            second_key = (!second_key)? key : second_key + " " + key;
                        } else {
                            third_key = (!third_key)? key : third_key + " " + key;
                        }
                    }
                }
                document.getElementById("combo_keyword_0").value = first_key;
                document.getElementById("combo_keyword_1").value = second_key;
                document.getElementById("combo_keyword_2").value = third_key;
            } else {
                // add value to first key combination
                document.getElementById("combo_keyword_0").value = cleaned_keys;
            }
        }
    }

    // [Click search tree] Change appearance (highlight) the search tree icon and toggle
    function toggleSearchTreeSelected() {
        $("#search-tree-icon").toggleClass("search-tree-icon-inactive search-tree-icon-active")
        $("#search-tree-icon-outer-div").toggleClass("search-tree-icon-outer-div-inactive search-tree-icon-outer-div-active")
        let query = $("#headerglobalsearchinput").val();

        if ($("#search-tree-icon").hasClass("search-tree-icon-active")) {
            $("#search-tree-toggle").prop("checked", true);
            $("#searchPath").val(getTreePath());
            $("#headerglobalsearchinput").attr("placeholder", `Search within: ... ${getTreePathPartial()}`);

            if (!query.includes(SOURCE_URI_SCOPE_PREFIX)) {
                const retainedSuri = retainedSourceUri();
                if (retainedSuri != "") {
                    if (query.trim() != "") {
                        query += " AND ";
                    }
                    query += `${SOURCE_URI_SCOPE_PREFIX}"${retainedSuri}"`;
                } else {
                    if (query.trim() != "") {
                        query += " AND ";
                    }
                    query += `${SOURCE_URI_SCOPE_PREFIX}"${getTreePath()}"`;
                }
                $("#headerglobalsearchinput").val(query);
            }
        } else {
            $("#search-tree-toggle").prop("checked", false);
            $("#searchPath").val("");
            $("#headerglobalsearchinput").attr("placeholder", "Search");

            if (query.includes(SOURCE_URI_SCOPE_PREFIX)) {
                let queryArray = query.split(" AND ");
                const sourceUriIndex = queryArray.findIndex((el) => el.startsWith(SOURCE_URI_SCOPE_PREFIX));

                if (sourceUriIndex != -1) {
                    queryArray.splice(sourceUriIndex, 1);
                    $("#headerglobalsearchinput").val(queryArray.join(" AND "));
                }
            }
        }
    }

    if ($("#search-tree-icon").hasClass("search-tree-icon-active")) {
        $("#headerglobalsearchinput").attr("placeholder", `Search within: ... ${getTreePathPartial()}`);
    } else {
        $("#headerglobalsearchinput").attr("placeholder", "Search");
    }


    $("#search-tree-icon").click(function(){toggleSearchTreeSelected()});

    // Press return key on search tree button enable/disable the search in path function
    $("#search-tree-icon").on("keyup", function(event){
        if (event.keyCode === 13) {
            event.preventDefault();
            toggleSearchTreeSelected();
        }
    });

    $("#search-tree-toggle").click(function(){toggleSearchTreeSelected()});

    $("#search-tree-info").popover({
        html: true,
        content:function() {
            return $('#search-tree-info-content-outer').html();
        }, 
        trigger: "click"
    });

    function getTreePathPartial() {
        const sourceUri = retainedSourceUri();
        if (sourceUri != "") {
            return sourceUri.substring(sourceUri.indexOf(VIEW_PAGE_PATH));
        }
        const fullPathAsArray = window.docviewurl.split("/");
        fullPathAsArray.pop();
        const path = fullPathAsArray.join("/");
        return path;
    }

    function retainedSourceUri() {
        const currentUrl = new URL(document.location);
        const searchParams = currentUrl.searchParams;
        const searchPath = searchParams.get(SEARCH_PATH_QUERY_PARAM_KEY);
        if (searchPath != null) {
            return searchPath;
        }

        const query = searchParams.get("text");
        if (query == null) {
            return "";
        }
        
        const queryArray = query.split(" AND ");
        const sourceUri = queryArray.find((el) => el.startsWith(SOURCE_URI_SCOPE_PREFIX));

        if (sourceUri == undefined) {
            return "";
        }
        
        return sourceUri.replace(SOURCE_URI_SCOPE_PREFIX, "").replaceAll("\"", "");
    }



    const sourceUri = retainedSourceUri();
    $("#search-tree-url-path").text(
        "..." + getTreePathPartial()
    );

    // [Click handle] Clear all form data when clicks "Clean selections"
    $("#advancedSearchClearAllBtn").on("click", function() {
        $(".xwiki-search-clearable-form").trigger("reset");

        $("#search-tree-toggle").removeAttr('checked');
        $("#search-tree-icon").removeClass("search-tree-icon-active")
        $("#search-tree-icon-outer-div").removeClass("search-tree-icon-outer-div-active")
        $("#search-tree-icon").addClass("search-tree-icon-inactive")
        $("#search-tree-icon-outer-div").addClass("search-tree-icon-outer-div-inactive")

        inputValidationAndInitializationHandler(combo_key0_label, null, KEYS_COMBO_GUIDE_TEXT);
        inputValidationAndInitializationHandler(combo_key1_label, null, KEYS_COMBO_GUIDE_TEXT);
        inputValidationAndInitializationHandler(combo_key2_label, null, KEYS_COMBO_GUIDE_TEXT);
        inputValidationAndInitializationHandler(wildcard_label, null, WILDCARD_GUIDE_TEXT);
        inputValidationAndInitializationHandler(alias_label, null, ALIAS_GUIDE_TEXT);
        inputValidationAndInitializationHandler(tag_label, null, TAGS_GUIDE_TEXT);
    });

    /**
     * [Search Click handle] Process and submit search request when click on Apply in Advanced Search Modal
     */

    // Check if input value is valid or not
    function isValidInputValue(value) {
        return (value != null && value !== "");
    }

    // Get processed keywords combo search text.
    // If user did not choose this method, return ""
    function getKeywordsComboText() {
        const textList = [];

        const combo0Keyword = $("#combo_keyword_0").val().trim();
        const combo1Keyword = $("#combo_keyword_1").val().trim();
        const combo2Keyword = $("#combo_keyword_2").val().trim();


        const op0Value = $('#combo_op_0').find(":selected").val();
        const op1Value = $('#combo_op_1').find(":selected").val();
        let countOp = 0;

        if (isValidInputValue(combo0Keyword)) {
            textList.push(combo0Keyword);
        }

        // Only append 1st op when we have 1st & 2nd keywords
        if (isValidInputValue(combo0Keyword) && isValidInputValue(op0Value) && isValidInputValue(combo1Keyword)) {
            textList.push(op0Value);
            countOp++;
        }

        if (isValidInputValue(combo1Keyword)) {
            textList.push(combo1Keyword);
        }

        // Only append 2nd op when we have 3rd keyword and 1st OR 2nd keyword
        if ((isValidInputValue(combo0Keyword) || isValidInputValue(combo1Keyword))
            && isValidInputValue(op1Value) && isValidInputValue(combo2Keyword)) {
            textList.push(op1Value);
            countOp++;
        }

        if (isValidInputValue(combo2Keyword)) {
            textList.push(combo2Keyword);
        }

        // Agreed with UX designer to group first two keywords with operator when there are 3 keywords & 2 operators
        if(countOp === 2) {
            textList.splice(0, 0, '(');
            textList.splice(4, 0, ')');
        }

        // join by space
        if (textList.length > 0) {
            return textList.join(" ");
        } else {
            return "";
        }
    }

    // Get processed wildcard search text.
    // If user did not choose this method, return ""
    function getWildCardText() {
        const wildcardKeyword = $("#wildcard_keyword").val().trim();
        if (isValidInputValue(wildcardKeyword)) {
            return wildcardKeyword;
        }
        return "";
    }

    function getUserAliasText() {
        const aliasKeyword = $("#creator_keyword").val().trim();
        if (isValidInputValue(aliasKeyword)) {
            return aliasKeyword;
        }
        return "";
    }

    function getTagsText() {
        const tagsKeyword = $("#tag_keyword").val().trim();
        if (isValidInputValue(tagsKeyword)) {
            return tagsKeyword;
        }
        return "";
    }


    // Validate user input in modal
    function validateModalInput() {
        for(element of advancedsearch_elements) {
            let element_id = "#".concat(element);
            if($(element_id).parent().is('.has-error')) {
                document.getElementById(element).focus();
                return false;
            }
        }
        return true;
    }
    
    function getTreePath() {
        let path = "";
        if ($("#search-tree-icon").hasClass("search-tree-icon-active")) {
            path = window.location.origin + getTreePathPartial();
        }
        return path;
    }

    function getAdvancedSearchQueryText() {
        const comboText = getKeywordsComboText();
        const wildcardText = getWildCardText();
        const aliasText = getUserAliasText();
        const tagsText = getTagsText();
        const treePath = getTreePath();

        let searchText = "";

        if (comboText !== "") {
            searchText = comboText;
        }

        if (wildcardText !== "") {
            if(searchText != "") {
                searchText = searchText  + " AND " + wildcardText;
            } else {
                searchText = wildcardText;
            }
        }

        if(aliasText != "") {
            let aliasQuery = "creator:\"" + aliasText + "\"" + " OR last_modifier:\"" + aliasText + "\"";
            if(searchText != "") {
                searchText = searchText + " AND ("+ aliasQuery + ")";
            } else {
                searchText = aliasQuery;
            }
        }

        if(tagsText != "") {
            let tagsQuery = "category_list:" + tagsText;
            if(searchText != "") {
                searchText = searchText + " AND " + tagsQuery;
            } else {
                searchText = tagsQuery;
            }
        }

        if (treePath != "") {
            if (searchText != "") {
                searchText = `${searchText} AND ${SOURCE_URI_SCOPE_PREFIX}\"${treePath}\"`;
            } else {
                searchText = `${SOURCE_URI_SCOPE_PREFIX}\"${treePath}\"`;
            }
        }
        
        return searchText;
    }

    function submitAdvancedQueryRequest() {
        if(!validateModalInput()) {
            return;
        }

        let searchText = getAdvancedSearchQueryText();

        if(searchText == "") {
            return;
        }

        // Update query text to advanced search query
        $("#searchTextInput").val(searchText);

        // Flag if query comes from advanced search
        $("#advancedSearchFlag").val("1");

        $("#submit-form").submit();
    }

    $("#formSubmitBtn").on("click", function(){
        submitAdvancedQueryRequest();
    });

    // Press return key fires this event
    $("#viewAdvancedSearchModal").on("keyup", function(event){
        if (event.keyCode === 13) {
            event.preventDefault();
            submitAdvancedQueryRequest();
        }
    });

    // Press return key on Advanced search button opens the modal
    $("#advanced-search-icon").on("keyup", function(event){
        if (event.keyCode === 13) {
            event.preventDefault();
            $('#viewAdvancedSearchModal').modal('show')
        }
    });
});
