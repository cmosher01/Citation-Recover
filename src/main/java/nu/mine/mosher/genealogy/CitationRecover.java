package nu.mine.mosher.genealogy;

import org.slf4j.*;

import java.nio.file.*;
import java.sql.*;

public class CitationRecover {
    private static final Logger LOG = LoggerFactory.getLogger(CitationRecover.class);

    public static void main(final String... args) throws SQLException {
        if (args.length < 2) {
            LOG.error("Missing required arguments: <old-good-ftm-tree>.ftm <new-bad-ancestry-tree>.ftm");
            System.exit(1);
        }
        update(Paths.get(args[0]), Paths.get(args[1]));
        LOG.debug("Program completed normally.");
    }

    private static void update(final Path pathReference, final Path pathUpdating) throws SQLException {
        LOG.debug("opening FTM tree file: {}", pathReference);
        try (
            final var dbRef = DriverManager.getConnection("jdbc:sqlite:"+pathReference);
            final var dbUpd = DriverManager.getConnection("jdbc:sqlite:"+pathUpdating)) {
            showCountOfPersonRecords(dbRef);
            showCountOfBadSources(dbRef);
            showBadSources(dbRef, dbUpd);
        }
    }

    private static void showCountOfPersonRecords(final Connection dbRef) throws SQLException {
        try (
            final var q = dbRef.prepareStatement("select count(*) from person");
            final var rs = q.executeQuery()) {
            while (rs.next()) {
                final var c = rs.getInt(1);
                LOG.debug("total count of person records: {}", c);
            }
        }
    }

    private static void showCountOfBadSources(final Connection dbRef) throws SQLException {
        try (
            final var q = dbRef.prepareStatement(
                "select count(*) from source where length(pagenumber) >= 250");
            final var rs = q.executeQuery()) {
            while (rs.next()) {
                final var c = rs.getInt(1);
                LOG.debug("count of potentially truncated citations: {}:", c);
            }
        }
    }

    private static void showBadSources(final Connection dbRef, Connection dbUpd) throws SQLException {
        try (
            final var q = dbRef.prepareStatement(
"""
    select
        s.id,
        length(s.pagenumber) len,
        replace(replace(s.pagenumber,char(10),' '),char(13),' ') pagenumber,
        mf.date
    from
        source s join
        (
            select
                max(sl.sourceid) sourceid,
                max(coalesce(f.date,0)) date
            from
                sourcelink sl join
                fact f on (f.id = sl.linkid and sl.linktableid = 2)
            group by
                sl.sourceid
        ) mf on (mf.sourceid = s.id)
    where
        length(s.pagenumber) >= 250
"""
            );
            final var rs = q.executeQuery()) {
            while (rs.next()) {
                final var idRef = rs.getLong("id");
                final var lenRef = rs.getInt("len");
                final var pagenumberRef = rs.getString("pagenumber");
                final var date = rs.getLong("date");
                LOG.info("==================================================================================================================================");
                LOG.debug("id: {}, length: {}, date: {}", idRef, lenRef, date);
                LOG.debug("       {}", pagenumberRef);

                reportMatchingStatus(dbRef, dbUpd, idRef, lenRef, pagenumberRef, date);
            }
        }
    }


    private static void reportMatchingStatus(Connection dbRef, Connection dbUpd, long idRef, int lenRef, String pagenumberRef, long date) throws SQLException {
        try (
            final var q = dbUpd.prepareStatement(
"""
    select
        count(*) c
    from
        source s join
        sourcelink sl on (sl.sourceid = s.id) join
        fact f on (f.id = sl.linkid and sl.linktableid = 2)
    where
        instr(?, s.pagenumber) = 1 and
        s.pagenumber != ? and
        f.date = ?
"""
            )) {
            q.setString(1, pagenumberRef);
            q.setString(2, pagenumberRef);
            q.setLong(3, date);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var c = rs.getInt("c");
                    if (c == 0) {
                        LOG.warn("        ref-id: {}, matches: {} <----------- Could not find any matches", idRef, c);
                        LOG.info("        {}", pagenumberRef);
                    } else if (c == 1) {
//                        LOG.debug("        ref-id: {}, matches: {}", idRef, c);
                        updateSource(dbRef, dbUpd, idRef, pagenumberRef, date);
                    } else if (1 < c) {
                        LOG.warn("        ref-id: {}, matches: {} <----------- Found multiple matches", idRef, c);
                        LOG.info("        {}", pagenumberRef);
                    }
                }
            }
        }
    }

    private static void updateSource(Connection dbRef, Connection dbUpd, long idRef, String pagenumberRef, final long date) throws SQLException {
        long idUpd = -1;
        try (
            final var q = dbUpd.prepareStatement(
                """
                        select
                            s.id
                        from
                            source s join
                            sourcelink sl on (sl.sourceid = s.id) join
                            fact f on (f.id = sl.linkid and sl.linktableid = 2)
                        where
                            instr(?, s.pagenumber) = 1 and
                            s.pagenumber != ? and
                            f.date = ?
                    """
            )) {
            q.setString(1, pagenumberRef);
            q.setString(2, pagenumberRef);
            q.setLong(3, date);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    idUpd = rs.getLong("id");
                    LOG.info("-----------------------------------");
                    LOG.info("will update TRUNCATED SOURCE (id: {})", idUpd);
//                    LOG.info("        {}", pagenumberUpd);
//                    LOG.info("-----------------------------------");
                    LOG.info("with REFERENCE SOURCE (id: {})", idRef);
                    LOG.info("        {}", pagenumberRef);
                }
            }
        }

        if (idUpd >= 0) {
            try (final var q = dbUpd.prepareStatement("update source set pagenumber = ? where id = ?")) {
                q.setString(1, pagenumberRef);
                q.setLong(2, idUpd);
                final var ret = q.executeUpdate();
                LOG.info("updated row count: {}", ret);
            }
        }
    }
}
