require(['jquery'], function($, _) {
    $(function(){
        const CURRENT_USER_ELEM = $('#current-user');
        const SAVE_IN_PROGRESS = $('#save-in-progress').attr('value');
        const SAVE_SUCCESS = $('#save-success').attr('value');
        const SAVE_ERROR = $('#save-error').attr('value');
        const savingBox = new XWiki.widgets.Notification(SAVE_IN_PROGRESS, "inprogress", {inactive: true});
        const savedBox = new XWiki.widgets.Notification(SAVE_SUCCESS, "done", {inactive: true});
        const errorBox = new XWiki.widgets.Notification(SAVE_ERROR, "error", {inactive: true});
        
        let SET_LANGUAGE_REST_URL = window.location.href.replace(/(?<=bin).*$/, "").replace('/bin', '') + '/rest/wikis/xwiki/localization/language?';
        
        postLanguagePreference = (languageClicked) => {
            savingBox.show();

            const userAlias = CURRENT_USER_ELEM.attr('value');
            const preferredLanguage = languageClicked;
            
            $.ajax({
                type: 'POST',
                contentType: 'application/json',
                url:  SET_LANGUAGE_REST_URL + $.param({
                    'user_alias': userAlias,
                    'preferred_language': preferredLanguage
                }),
                error: function() {
                    savingBox.replace(errorBox);
                },
                success: function(data) {
                    savingBox.replace(savedBox);
                    location.reload(true); 
                }
            })
        }


        $(".localeOption").click(function(e) {
            postLanguagePreference(e.currentTarget.getAttribute("data-locale-id"));
          });

    });
})();
