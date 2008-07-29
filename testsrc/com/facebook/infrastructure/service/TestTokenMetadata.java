package com.facebook.infrastructure.service;

import java.math.BigInteger;

import junit.framework.TestCase;

import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.net.EndPoint;

public class TestTokenMetadata extends TestCase {    
    private static final BigInteger TOK_0A = new BigInteger("500");
    private static final BigInteger TOK_A = new BigInteger("1000");
    private static final BigInteger TOK_AB = new BigInteger("1500");
    private static final BigInteger TOK_B = new BigInteger("2000");
    private static final BigInteger TOK_BC = new BigInteger("2500");
    private static final BigInteger TOK_C = new BigInteger("3000");
    private static final BigInteger TOK_CD = new BigInteger("3500");

    private static final EndPoint EP_A = new EndPoint("host-a", 1);
    private static final EndPoint EP_B = new EndPoint("host-b", 1);
    private static final EndPoint EP_C = new EndPoint("host-c", 1);


    private TokenMetadata buildRing()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd = tmd.update(TOK_A, EP_A);
        tmd = tmd.update(TOK_B, EP_B);
        tmd = tmd.update(TOK_C, EP_C);
        return tmd;
    }

    public void testBuild() {
        TokenMetadata tmd = buildRing();
    }

    public void testBasic() {
        TokenMetadata tmd = buildRing();
        assertEquals(3, tmd.getRingSize());

        assertEquals(TOK_A, tmd.getToken(EP_A));
        assertEquals(TOK_B, tmd.getToken(EP_B));
        assertEquals(TOK_C, tmd.getToken(EP_C));

        // Check tokenBefore for tokens on the ring
        assertEquals(TOK_A, tmd.getTokenBefore(TOK_B));
        assertEquals(TOK_B, tmd.getTokenBefore(TOK_C));
        assertEquals(TOK_C, tmd.getTokenBefore(TOK_A));

        // Check tokenBefore for tokens in between
        assertEquals(TOK_C, tmd.getTokenBefore(TOK_0A));
        assertEquals(TOK_A, tmd.getTokenBefore(TOK_AB));
        assertEquals(TOK_B, tmd.getTokenBefore(TOK_BC));
        assertEquals(TOK_C, tmd.getTokenBefore(TOK_CD));

        // Check tokenBefore for endpoints on the ring
        assertEquals(EP_A, tmd.getTokenBefore(EP_B));
        assertEquals(EP_B, tmd.getTokenBefore(EP_C));
        assertEquals(EP_C, tmd.getTokenBefore(EP_A));

        // Check getTokenAfter for tokens on the ring
        assertEquals(TOK_B, tmd.getTokenAfter(TOK_A));
        assertEquals(TOK_C, tmd.getTokenAfter(TOK_B));
        assertEquals(TOK_A, tmd.getTokenAfter(TOK_C));

        // Check getTokenAfter for tokens in between
        assertEquals(TOK_A, tmd.getTokenAfter(TOK_CD));
        assertEquals(TOK_A, tmd.getTokenAfter(TOK_0A));
        assertEquals(TOK_B, tmd.getTokenAfter(TOK_AB));
        assertEquals(TOK_C, tmd.getTokenAfter(TOK_BC));
        
        // Check tokenAfter for endpoints on the ring
        assertEquals(EP_B, tmd.getTokenAfter(EP_A));
        assertEquals(EP_C, tmd.getTokenAfter(EP_B));
        assertEquals(EP_A, tmd.getTokenAfter(EP_C));        

        // Check getTokenIndex
        assertEquals(0, tmd.getTokenIndex(TOK_0A));
        assertEquals(0, tmd.getTokenIndex(TOK_A));
        assertEquals(1, tmd.getTokenIndex(TOK_AB));
        assertEquals(1, tmd.getTokenIndex(TOK_B));
        assertEquals(2, tmd.getTokenIndex(TOK_BC));
        assertEquals(2, tmd.getTokenIndex(TOK_C));
        assertEquals(0, tmd.getTokenIndex(TOK_CD));

        // Check getResponsibleEndPoint
        assertEquals(EP_A, tmd.getResponsibleEndPoint(TOK_0A));
        assertEquals(EP_A, tmd.getResponsibleEndPoint(TOK_A));

        assertEquals(EP_B, tmd.getResponsibleEndPoint(TOK_AB));
        assertEquals(EP_B, tmd.getResponsibleEndPoint(TOK_B));

        assertEquals(EP_C, tmd.getResponsibleEndPoint(TOK_BC));
        assertEquals(EP_C, tmd.getResponsibleEndPoint(TOK_C));

        assertEquals(EP_A, tmd.getResponsibleEndPoint(TOK_CD));

        // Check getResponsibleToken
        assertEquals(TOK_A, tmd.getResponsibleToken(TOK_0A));
        assertEquals(TOK_A, tmd.getResponsibleToken(TOK_A));

        assertEquals(TOK_B, tmd.getResponsibleToken(TOK_AB));
        assertEquals(TOK_B, tmd.getResponsibleToken(TOK_B));

        assertEquals(TOK_C, tmd.getResponsibleToken(TOK_BC));
        assertEquals(TOK_C, tmd.getResponsibleToken(TOK_C));

        assertEquals(TOK_A, tmd.getResponsibleToken(TOK_CD));

        // Check isKnownEndPoint
        assertTrue(tmd.isKnownEndPoint(EP_A));
        assertTrue(tmd.isKnownEndPoint(EP_B));
        assertTrue(tmd.isKnownEndPoint(EP_C));
    }

    public void testRanges()
    {
        Range[] ranges = buildRing().getRanges();
        assertEquals(3, ranges.length);

        assertEquals(new Range(TOK_A, TOK_B), ranges[0]);
        assertEquals(new Range(TOK_B, TOK_C), ranges[1]);
        assertEquals(new Range(TOK_C, TOK_A), ranges[2]);
    }

    private void checkIter(BigInteger[] expectedToks,
                           TokenMetadata.RingIterator iter)
    {
        for (int i = 0; i < expectedToks.length; i++)
        {
            assertTrue( iter.hasNext() );
            BigInteger tok = iter.next().getKey();
            assertEquals(expectedToks[i], tok);
        }
        assertFalse( iter.hasNext() );
    }

    public void testIterator()
    {
        TokenMetadata tmd = buildRing();

        checkIter(new BigInteger[] {TOK_A, TOK_B, TOK_C},
                  tmd.getRingIterator(TOK_0A));
        checkIter(new BigInteger[] {TOK_A, TOK_B, TOK_C},
                  tmd.getRingIterator(TOK_A));
        checkIter(new BigInteger[] {TOK_B, TOK_C, TOK_A},
                  tmd.getRingIterator(TOK_AB));
        checkIter(new BigInteger[] {TOK_C, TOK_A, TOK_B},
                  tmd.getRingIterator(TOK_C));
        checkIter(new BigInteger[] {TOK_A, TOK_B, TOK_C},
                  tmd.getRingIterator(TOK_CD));

        checkIter(new BigInteger[] {TOK_A, TOK_B, TOK_C},
                  tmd.getRingIterator(EP_A));
    }
}