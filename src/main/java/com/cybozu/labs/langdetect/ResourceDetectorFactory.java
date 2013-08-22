package com.cybozu.labs.langdetect;

import java.io.IOException;
import java.io.InputStream;

import net.arnx.jsonic.JSON;
import net.arnx.jsonic.JSONException;

import com.cybozu.labs.langdetect.util.LangProfile;

/**
 * Wrapper for DetectorFactory, allowing us to load profiles from
 * resources instead of the local file system.
 */
public class ResourceDetectorFactory {
    public static String[] profiles = {
        "com/cybozu/labs/langdetect/profiles/af",
        "com/cybozu/labs/langdetect/profiles/ar",
        "com/cybozu/labs/langdetect/profiles/bg",
        "com/cybozu/labs/langdetect/profiles/bn",
        "com/cybozu/labs/langdetect/profiles/cs",
        "com/cybozu/labs/langdetect/profiles/da",
        "com/cybozu/labs/langdetect/profiles/de",
        "com/cybozu/labs/langdetect/profiles/el",
        "com/cybozu/labs/langdetect/profiles/en",
        "com/cybozu/labs/langdetect/profiles/es",
        "com/cybozu/labs/langdetect/profiles/et",
        "com/cybozu/labs/langdetect/profiles/fa",
        "com/cybozu/labs/langdetect/profiles/fi",
        "com/cybozu/labs/langdetect/profiles/fr",
        "com/cybozu/labs/langdetect/profiles/gu",
        "com/cybozu/labs/langdetect/profiles/he",
        "com/cybozu/labs/langdetect/profiles/hi",
        "com/cybozu/labs/langdetect/profiles/hr",
        "com/cybozu/labs/langdetect/profiles/hu",
        "com/cybozu/labs/langdetect/profiles/id",
        "com/cybozu/labs/langdetect/profiles/it",
        "com/cybozu/labs/langdetect/profiles/ja",
        "com/cybozu/labs/langdetect/profiles/kn",
        "com/cybozu/labs/langdetect/profiles/ko",
        "com/cybozu/labs/langdetect/profiles/lt",
        "com/cybozu/labs/langdetect/profiles/lv",
        "com/cybozu/labs/langdetect/profiles/mk",
        "com/cybozu/labs/langdetect/profiles/ml",
        "com/cybozu/labs/langdetect/profiles/mr",
        "com/cybozu/labs/langdetect/profiles/ne",
        "com/cybozu/labs/langdetect/profiles/nl",
        "com/cybozu/labs/langdetect/profiles/no",
        "com/cybozu/labs/langdetect/profiles/pa",
        "com/cybozu/labs/langdetect/profiles/pl",
        "com/cybozu/labs/langdetect/profiles/pt",
        "com/cybozu/labs/langdetect/profiles/ro",
        "com/cybozu/labs/langdetect/profiles/ru",
        "com/cybozu/labs/langdetect/profiles/sk",
        "com/cybozu/labs/langdetect/profiles/sl",
        "com/cybozu/labs/langdetect/profiles/so",
        "com/cybozu/labs/langdetect/profiles/sq",
        "com/cybozu/labs/langdetect/profiles/sv",
        "com/cybozu/labs/langdetect/profiles/sw",
        "com/cybozu/labs/langdetect/profiles/ta",
        "com/cybozu/labs/langdetect/profiles/te",
        "com/cybozu/labs/langdetect/profiles/th",
        "com/cybozu/labs/langdetect/profiles/tl",
        "com/cybozu/labs/langdetect/profiles/tr",
        "com/cybozu/labs/langdetect/profiles/uk",
        "com/cybozu/labs/langdetect/profiles/ur",
        "com/cybozu/labs/langdetect/profiles/vi",
        "com/cybozu/labs/langdetect/profiles/zh-cn",
        "com/cybozu/labs/langdetect/profiles/zh-tw" };


    public static synchronized void loadProfile() throws LangDetectException {
        ClassLoader cl = ResourceDetectorFactory.class.getClassLoader();
        int langsize = profiles.length;
        int index = 0;
        for (String path : profiles) {
            InputStream is = null;
            try {
                is = cl.getResourceAsStream(path);
                LangProfile profile = JSON.decode(is, LangProfile.class);
                DetectorFactory.addProfile(profile, index, langsize);
                ++index;
            } catch (JSONException e) {
                throw new LangDetectException(ErrorCode.FormatError, "profile format error in '" + path + "'");
            } catch (IOException e) {
                throw new LangDetectException(ErrorCode.FileLoadError, "can't open '" + path + "'");
            } finally {
                try {
                    if (is!=null) is.close();
                } catch (IOException e) {}
            }
        }
    }
}
