-- ✅ 設定：変換したい .numbers が入っているフォルダ
property baseFolderPOSIX : "/Users/harukishiroyama/Downloads/road_companiesInfo/csv/new"

on run
    -- 変換対象フォルダを alias に変換
    set baseFolderAlias to POSIX file baseFolderPOSIX as alias

    -- 再帰的に .numbers ファイル一覧を取得
    set numbersFiles to my collectNumbersFiles(baseFolderAlias)

    if (count of numbersFiles) = 0 then
        display dialog "対象フォルダ内に .numbers ファイルがありません。" buttons {"OK"} default button 1
        return
    end if

    display dialog "合計 " & (count of numbersFiles) & " 件の .numbers を CSV に書き出します。" buttons {"OK"} default button 1

    tell application "Numbers"
        repeat with f in numbersFiles
            set anAlias to f as alias
            set doc to open anAlias

            -- ★ ドキュメントの path ではなく「開いたファイルの alias」からパスを取る
            set hfsPath to (anAlias as text) -- "Macintosh HD:Users:..." 形式のパス

            -- alias の文字列表現は最後が ":" で終わるので、それを外してから .csv を付ける
            if hfsPath ends with ":" then
                set hfsPath to text 1 thru -2 of hfsPath
            end if

            -- 元ファイルと同じ場所に「1.numbers.csv」のような名前で出力する
            set csvPath to hfsPath & ".csv"

            -- CSV として書き出し
            export doc to file csvPath as CSV

            close doc saving no
        end repeat
    end tell

    display dialog "すべての .numbers を CSV に書き出しました。" buttons {"OK"} default button 1
end run

-- 再帰的に .numbers ファイルを集めるハンドラ
on collectNumbersFiles(aFolderAlias)
    set resultList to {}
    tell application "Finder"
        set itemList to every item of aFolderAlias
        repeat with itRef in itemList
            if class of itRef is folder then
                set resultList to resultList & my collectNumbersFiles(itRef as alias)
            else
                set fn to name of itRef as text
                if fn ends with ".numbers" then
                    set end of resultList to (itRef as alias)
                end if
            end if
        end repeat
    end tell
    return resultList
end collectNumbersFiles